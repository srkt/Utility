#!/usr/bin/env python3
# ============================================================
# CES Savings ETL - Main Orchestrator
# Entry point for the monthly ETL pipeline
# Called by Jenkins or manually
#
# Fix V2: Multi-sheet bug fixed
#         File now archived AFTER both sheets processed
#         Not after first sheet
# ============================================================

import sys
import os
import argparse
import logging
from pathlib import Path

# Add project root to path
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

# Load config
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
    Process one sheet from a vendor file.

    IMPORTANT: Does NOT archive the file.
    Archiving happens in run_pipeline AFTER all sheets
    from the same file are processed successfully.

    Args:
        file_info  : dict with file path, vendor key, period info
        sheet_key  : 'savings_results' or 'savings_forecast'
        sheet_type : 'ACTUALS' or 'FORECAST'
        batch_id   : current batch id from ETL_BATCH_LOG
        audit      : AuditLogger instance

    Returns:
        dict with processing stats
    """
    file_path    = file_info["file_path"]
    sheet_config = CONFIG["excel"]["sheets"][sheet_key]
    sheet_name   = sheet_config["sheet_name"]

    stats = {
        "status":          "FAILED",
        "rows_source":     0,
        "rows_loaded":     0,
        "rows_failed":     0,
        "is_resubmission": False
    }

    # Start file audit log
    file_log_id = audit.start_file(
        batch_id         = batch_id,
        file_name        = file_info["file_name"],
        file_type        = sheet_type,
        vendor_key       = file_info["vendor_key"],
        reporting_period = file_info["reporting_period"],
        file_path_source = str(file_path)
    )

    try:
        # Load column mapping for this sheet from Oracle
        column_mapping = db.load_column_mapping(sheet_name)
        if not column_mapping:
            raise ValueError(
                f"No column mapping found for sheet '{sheet_name}'. "
                f"Run ces_savings_column_map.sql first."
            )

        # --------------------------------------------------------
        # STEP 1: EXTRACT
        # Read Excel file, map 3-row headers to Oracle columns
        # Stop at blank VENDOR_SUBPROGRAM_KEY
        # --------------------------------------------------------
        extractor = ExcelExtractor(column_mapping, sheet_config)

        if CONFIG["etl"].get("validate_template_version"):
            extractor.validate_template_version(file_path)

        df, rows_source = extractor.extract(file_path)
        stats["rows_source"] = rows_source

        # --------------------------------------------------------
        # STEP 2: VALIDATE
        # Check required columns, numeric types
        # Collect all errors before deciding pass/fail
        # --------------------------------------------------------
        validator = DataValidator(
            column_mapping,
            max_errors=CONFIG["etl"].get("max_errors_per_file", 100)
        )
        val_result = validator.validate(df)

        # Log every row error to ETL_ROW_LOG
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
            # Too many errors - mark as quarantine
            # Actual file move happens in run_pipeline
            # after all sheets checked
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
                f"  Sheet {sheet_key} failed validation: "
                f"{val_result.error_summary}"
            )
            return stats

        # --------------------------------------------------------
        # STEP 3: TRANSFORM
        # Cast types, preserve NULL, add metadata
        # --------------------------------------------------------
        transformer = DataTransformer(column_mapping)
        df_clean = transformer.transform(df, file_info, batch_id)

        # --------------------------------------------------------
        # STEP 4: LOAD
        # STG first, then FACT with dimension lookups
        # Handles resubmission versioning automatically
        # --------------------------------------------------------
        loader = DataLoader(db, column_mapping, sheet_type)
        loader.load_to_stg(df_clean, batch_id)
        rows_loaded, is_resub = loader.load_to_fact(df_clean, file_info, batch_id)

        stats["rows_loaded"]     = rows_loaded
        stats["is_resubmission"] = is_resub
        stats["rows_failed"]     = val_result.failed_rows
        stats["status"]          = "SUCCESS"

        # Update file log - no archive path yet
        # Archive path set in run_pipeline after all sheets done
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
            f"  Sheet {sheet_key}: loaded {rows_loaded} rows "
            f"({'resubmission' if is_resub else 'new'})"
        )

    except Exception as e:
        logger.exception(
            f"  Error processing {file_info['file_name']} "
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
    jenkins_job:   str  = None,
    build_number:  str  = None,
    sheets:        list = None
) -> int:
    """
    Run the full monthly ETL pipeline.

    Flow per file:
    1. Process Savings Results sheet (actuals)
    2. Process Savings Forecast sheet (forecast)
    3. Archive file if BOTH sheets succeeded
    4. Quarantine file if EITHER sheet failed

    Args:
        jenkins_job  : Jenkins job name for audit trail
        build_number : Jenkins build number for audit trail
        sheets       : list of sheet keys, default both

    Returns:
        0 = success or partial
        1 = complete failure
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

    # Start batch audit record
    batch_id = audit.start_batch(jenkins_job, build_number)
    logger.info(f"Pipeline started: BATCH_ID={batch_id}")

    # Aggregate stats across all files
    agg = {
        "files_expected":    0,
        "files_succeeded":   0,
        "files_failed":      0,
        "files_quarantined": 0,
        "rows_loaded":       0,
        "rows_failed":       0
    }

    try:
        # Scan landing zone for vendor files
        files = file_mgr.scan_landing_zone()
        agg["files_expected"] = len(files)

        if not files:
            logger.warning("No files found in landing zone")

        # Process each vendor file
        for file_info in files:
            logger.info(
                f"Processing: {file_info['file_name']} "
                f"vendor={file_info['vendor_key']} "
                f"period={file_info['reporting_period']}"
            )

            # Collect results for each sheet
            # Process ALL sheets before touching the file
            sheet_results = {}

            for sheet_key in sheets:
                sheet_type = sheet_type_map[sheet_key]
                logger.info(f"  Processing sheet: {sheet_key}")

                stats = process_file(
                    file_info  = file_info,
                    sheet_key  = sheet_key,
                    sheet_type = sheet_type,
                    batch_id   = batch_id,
                    audit      = audit
                )

                sheet_results[sheet_key] = stats
                agg["rows_loaded"] += stats["rows_loaded"]
                agg["rows_failed"] += stats["rows_failed"]

            # -----------------------------------------------
            # DECIDE WHAT TO DO WITH THE FILE
            # Only now, after ALL sheets processed
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
                # Both sheets loaded cleanly → archive file
                archive_path = file_mgr.archive_file(
                    file_info["file_path"]
                )
                agg["files_succeeded"] += 1
                logger.info(f"  ✓ Archived: {archive_path}")

            elif any_problem:
                # One or both sheets failed → quarantine file
                reason = " | ".join(
                    f"{k}: {v['status']}"
                    for k, v in sheet_results.items()
                )
                q_path = file_mgr.quarantine_file(
                    file_info["file_path"],
                    reason
                )
                # Count quarantine vs failed
                if any(r["status"] == "QUARANTINE" for r in sheet_results.values()):
                    agg["files_quarantined"] += 1
                else:
                    agg["files_failed"] += 1
                logger.warning(f"  ✗ Quarantined: {q_path} REASON: {reason}")

            else:
                agg["files_failed"] += 1

        # --------------------------------------------------------
        # FINALIZE BATCH
        # --------------------------------------------------------
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

        # Final summary
        logger.info("=" * 50)
        logger.info(f"Pipeline complete: {status}")
        logger.info(f"Files: {agg['files_succeeded']} succeeded, "
                    f"{agg['files_failed']} failed, "
                    f"{agg['files_quarantined']} quarantined")
        logger.info(f"Rows:  {agg['rows_loaded']} loaded, "
                    f"{agg['rows_failed']} failed")
        logger.info("=" * 50)

        return 0 if status in ("SUCCESS", "PARTIAL") else 1

    except Exception as e:
        logger.exception(f"Pipeline failed unexpectedly: {e}")
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
        description="CES Savings ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --sheets savings_results
  python main.py --sheets savings_results savings_forecast
  python main.py --jenkins-job CES_ETL --build-number 42
        """
    )
    parser.add_argument(
        "--jenkins-job",
        help="Jenkins job name (for audit trail)",
        default=None
    )
    parser.add_argument(
        "--build-number",
        help="Jenkins build number (for audit trail)",
        default=None
    )
    parser.add_argument(
        "--sheets",
        nargs="+",
        choices=["savings_results", "savings_forecast"],
        help="Sheets to process (default: both)",
        default=None
    )
    args = parser.parse_args()

    exit_code = run_pipeline(
        jenkins_job  = args.jenkins_job,
        build_number = args.build_number,
        sheets       = args.sheets
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
