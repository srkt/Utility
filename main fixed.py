#!/usr/bin/env python3
# ============================================================
# CES Savings ETL - Main Orchestrator v3
# Fixed: archive happens AFTER both sheets processed
# process_file() never archives or quarantines
# run_pipeline() handles file movement after all sheets done
# ============================================================

import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.db import db
from utils.logger import setup_logger, AuditLogger
from utils.file_manager import FileManager
from utils.notifier import Notifier
from etl.extractor import ExcelExtractor
from etl.validator import DataValidator
from etl.transformer import DataTransformer
from etl.loader import DataLoader

import yaml

config_path = Path(__file__).parent / "config" / "pipeline_config.yaml"
with open(config_path) as f:
    CONFIG = yaml.safe_load(f)

logger = setup_logger("ces_etl")


def process_file(
    file_info: dict,
    sheet_key: str,
    sheet_type: str,
    batch_id: int,
    audit: AuditLogger
) -> dict:
    """
    Process ONE sheet from a vendor file.

    Does NOT touch the file (no archive, no quarantine).
    File movement is handled by run_pipeline() AFTER
    all sheets from the same file are processed.

    Returns dict with processing stats.
    """
    file_path    = Path(file_info["file_path"])
    sheet_config = CONFIG["excel"]["sheets"][sheet_key]
    sheet_name   = sheet_config["sheet_name"]

    stats = {
        "status":          "FAILED",
        "rows_source":     0,
        "rows_loaded":     0,
        "rows_failed":     0,
        "is_resubmission": False
    }

    # Verify file still exists before processing
    if not file_path.exists():
        logger.error(
            f"File not found: {file_path}\n"
            f"File may have been moved by a previous step."
        )
        stats["status"] = "FAILED"
        return stats

    file_log_id = audit.start_file(
        batch_id         = batch_id,
        file_name        = file_info["file_name"],
        file_type        = sheet_type,
        vendor_key       = file_info["vendor_key"],
        reporting_period = file_info["reporting_period"],
        file_path_source = str(file_path)
    )

    try:
        # Load column mapping from Oracle
        column_mapping = db.load_column_mapping(sheet_name)
        if not column_mapping:
            raise ValueError(
                f"No column mapping found for sheet '{sheet_name}'. "
                f"Run ces_savings_column_map.sql first."
            )

        # 1. EXTRACT
        extractor = ExcelExtractor(column_mapping, sheet_config)
        if CONFIG["etl"].get("validate_template_version"):
            extractor.validate_template_version(file_path)
        df, rows_source = extractor.extract(file_path)
        stats["rows_source"] = rows_source

        # 2. VALIDATE
        validator = DataValidator(
            column_mapping,
            max_errors=CONFIG["etl"].get("max_errors_per_file", 100)
        )
        val_result = validator.validate(df)

        # Log row errors to ETL_ROW_LOG
        for err in val_result.errors:
            audit.log_row_error(
                file_log_id    = file_log_id,
                batch_id       = batch_id,
                row_number     = err["row_number"],
                column_name    = err["column_name"],
                source_value   = err["source_value"],
                error_type     = err["error_type"],
                error_message  = err["error_message"],
                expected_format= err.get("expected_format")
            )

        if not val_result.passed:
            # Mark as quarantine but DO NOT move file yet
            # run_pipeline() moves it after all sheets checked
            audit.complete_file(
                file_log_id      = file_log_id,
                status           = "QUARANTINE",
                row_count_source = rows_source,
                row_count_failed = val_result.failed_rows,
                validation_errors= val_result.error_summary
            )
            stats["status"]     = "QUARANTINE"
            stats["rows_failed"] = val_result.failed_rows
            logger.warning(
                f"  Sheet {sheet_key} validation failed: "
                f"{val_result.error_summary}"
            )
            return stats

        # 3. TRANSFORM
        transformer = DataTransformer(column_mapping)
        df_clean = transformer.transform(df, file_info, batch_id)

        # 4. LOAD to STG then FACT
        loader = DataLoader(db, column_mapping, sheet_type)
        loader.load_to_stg(df_clean, batch_id)
        rows_loaded, is_resub = loader.load_to_fact(df_clean, file_info, batch_id)

        stats["rows_loaded"]     = rows_loaded
        stats["is_resubmission"] = is_resub
        stats["rows_failed"]     = val_result.failed_rows
        stats["status"]          = "SUCCESS"

        audit.complete_file(
            file_log_id      = file_log_id,
            status           = "RESUBMISSION" if is_resub else "SUCCESS",
            row_count_source = rows_source,
            row_count_loaded = rows_loaded,
            row_count_failed = val_result.failed_rows,
            is_resubmission  = "Y" if is_resub else "N",
            validation_errors= val_result.error_summary if val_result.errors else None
        )

        logger.info(
            f"  Sheet {sheet_key}: {rows_loaded} rows loaded "
            f"({'resubmission' if is_resub else 'new'})"
        )

    except Exception as e:
        logger.exception(
            f"  Error on {file_info['file_name']} "
            f"sheet={sheet_key}: {e}"
        )
        audit.complete_file(
            file_log_id     = file_log_id,
            status          = "FAILED",
            row_count_source= stats["rows_source"],
            error_message   = str(e)[:4000]
        )
        stats["status"] = "FAILED"

    return stats


def run_pipeline(
    jenkins_job:  str  = None,
    build_number: str  = None,
    sheets:       list = None
) -> int:
    """
    Monthly ETL pipeline.

    Per file flow:
      1. Process Savings Results tab (actuals)
      2. Process Savings Forecast tab (forecast)
      3. If BOTH succeeded  → archive file
      4. If EITHER failed   → quarantine file

    File is never touched until ALL tabs are processed.
    """
    if sheets is None:
        sheets = ["savings_results", "savings_forecast"]

    sheet_type_map = {
        "savings_results":  "ACTUALS",
        "savings_forecast": "FORECAST"
    }

    audit    = AuditLogger(db)
    notifier = Notifier()
    file_mgr = FileManager()

    batch_id = audit.start_batch(jenkins_job, build_number)
    logger.info(f"Pipeline started: BATCH_ID={batch_id}")

    agg = {
        "files_expected":    0,
        "files_succeeded":   0,
        "files_failed":      0,
        "files_quarantined": 0,
        "rows_loaded":       0,
        "rows_failed":       0
    }

    try:
        files = file_mgr.scan_landing_zone()
        agg["files_expected"] = len(files)

        if not files:
            logger.warning("No files found in landing zone")

        for file_info in files:
            logger.info(
                f"Processing: {file_info['file_name']} "
                f"vendor={file_info['vendor_key']} "
                f"period={file_info['reporting_period']}"
            )

            # Collect results for ALL sheets first
            # File stays in landing zone throughout this loop
            sheet_results = {}

            for sheet_key in sheets:
                sheet_type = sheet_type_map[sheet_key]
                logger.info(f"  Sheet: {sheet_key}")

                stats = process_file(
                    file_info  = file_info,
                    sheet_key  = sheet_key,
                    sheet_type = sheet_type,
                    batch_id   = batch_id,
                    audit      = audit
                    # No file_mgr here - file not touched inside process_file
                )

                sheet_results[sheet_key] = stats
                agg["rows_loaded"] += stats["rows_loaded"]
                agg["rows_failed"] += stats["rows_failed"]

            # -----------------------------------------------
            # ALL SHEETS DONE - NOW decide what to do with file
            # -----------------------------------------------
            all_succeeded = all(
                r["status"] == "SUCCESS"
                for r in sheet_results.values()
            )
            any_problem = any(
                r["status"] in ("FAILED", "QUARANTINE")
                for r in sheet_results.values()
            )

            if all_succeeded:
                # Both tabs loaded → archive file
                archive_path = file_mgr.archive_file(
                    file_info["file_path"]
                )
                agg["files_succeeded"] += 1
                logger.info(f"  ✓ Archived: {archive_path}")

            elif any_problem:
                # One or both tabs failed → quarantine file
                reason = " | ".join(
                    f"{k}: {v['status']}"
                    for k, v in sheet_results.items()
                )
                q_path = file_mgr.quarantine_file(
                    file_info["file_path"],
                    reason
                )
                if any(r["status"] == "QUARANTINE"
                       for r in sheet_results.values()):
                    agg["files_quarantined"] += 1
                else:
                    agg["files_failed"] += 1
                logger.warning(
                    f"  ✗ Quarantined: {q_path} | {reason}"
                )

            else:
                agg["files_failed"] += 1

        # Batch status
        if agg["files_failed"] == 0 and agg["files_quarantined"] == 0:
            status = "SUCCESS"
        elif agg["files_succeeded"] == 0:
            status = "FAILED"
        else:
            status = "PARTIAL"

        agg["status"] = status

        audit.complete_batch(
            batch_id         = batch_id,
            status           = status,
            files_expected   = agg["files_expected"],
            files_succeeded  = agg["files_succeeded"],
            files_failed     = agg["files_failed"],
            files_skipped    = 0,
            files_quarantined= agg["files_quarantined"],
            rows_loaded      = agg["rows_loaded"],
            rows_failed      = agg["rows_failed"]
        )

        notifier.notify_batch_complete(batch_id, agg)

        logger.info("=" * 50)
        logger.info(f"Pipeline complete: {status}")
        logger.info(
            f"Files: {agg['files_succeeded']} succeeded, "
            f"{agg['files_failed']} failed, "
            f"{agg['files_quarantined']} quarantined"
        )
        logger.info(
            f"Rows: {agg['rows_loaded']} loaded, "
            f"{agg['rows_failed']} failed"
        )
        logger.info("=" * 50)

        return 0 if status in ("SUCCESS", "PARTIAL") else 1

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        agg["status"] = "FAILED"
        audit.complete_batch(
            batch_id         = batch_id,
            status           = "FAILED",
            files_expected   = agg["files_expected"],
            files_succeeded  = agg["files_succeeded"],
            files_failed     = agg["files_failed"],
            files_skipped    = 0,
            files_quarantined= agg["files_quarantined"],
            rows_loaded      = agg["rows_loaded"],
            rows_failed      = agg["rows_failed"],
            error_message    = str(e)[:4000]
        )
        notifier.notify_batch_complete(batch_id, agg)
        return 1

    finally:
        db.close_pool()


def main():
    parser = argparse.ArgumentParser(
        description="CES Savings ETL Pipeline"
    )
    parser.add_argument(
        "--jenkins-job", default=None,
        help="Jenkins job name"
    )
    parser.add_argument(
        "--build-number", default=None,
        help="Jenkins build number"
    )
    parser.add_argument(
        "--sheets", nargs="+",
        choices=["savings_results", "savings_forecast"],
        help="Sheets to process (default: both)",
        default=None
    )
    args = parser.parse_args()

    sys.exit(run_pipeline(
        jenkins_job  = args.jenkins_job,
        build_number = args.build_number,
        sheets       = args.sheets
    ))


if __name__ == "__main__":
    main()
