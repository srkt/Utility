#!/usr/bin/env python3
# ============================================================
# CES Savings ETL - Main Orchestrator
# Entry point for the monthly ETL pipeline
# Called by Jenkins or manually
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
    audit: AuditLogger,
    file_mgr: FileManager
) -> dict:
    """
    Process a single vendor file for one sheet (actuals or forecast).

    Returns dict with processing stats.
    """
    file_path = file_info["file_path"]
    sheet_config = CONFIG["excel"]["sheets"][sheet_key]
    sheet_name = sheet_config["sheet_name"]

    stats = {
        "status": "FAILED",
        "rows_source": 0,
        "rows_loaded": 0,
        "rows_failed": 0,
        "is_resubmission": False
    }

    # Start file audit log
    file_log_id = audit.start_file(
        batch_id=batch_id,
        file_name=file_info["file_name"],
        file_type=sheet_type,
        vendor_key=file_info["vendor_key"],
        reporting_period=file_info["reporting_period"],
        file_path_source=str(file_path)
    )

    try:
        # Load column mapping for this sheet
        column_mapping = db.load_column_mapping(sheet_name)
        if not column_mapping:
            raise ValueError(f"No column mapping found for sheet '{sheet_name}'")

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

        # Log row errors
        for err in val_result.errors:
            audit.log_row_error(
                file_log_id=file_log_id,
                batch_id=batch_id,
                row_number=err["row_number"],
                column_name=err["column_name"],
                source_value=err["source_value"],
                error_type=err["error_type"],
                error_message=err["error_message"],
                expected_format=err.get("expected_format")
            )

        if not val_result.passed:
            # Quarantine file
            q_path = file_mgr.quarantine_file(file_path, val_result.error_summary)
            audit.complete_file(
                file_log_id=file_log_id,
                status="QUARANTINE",
                row_count_source=rows_source,
                row_count_failed=val_result.failed_rows,
                file_path_quarantine=str(q_path),
                validation_errors=val_result.error_summary
            )
            stats["status"] = "QUARANTINE"
            stats["rows_failed"] = val_result.failed_rows
            return stats

        # 3. TRANSFORM
        transformer = DataTransformer(column_mapping)
        df_clean = transformer.transform(df, file_info, batch_id)

        # 4. LOAD
        loader = DataLoader(db, column_mapping, sheet_type)
        loader.load_to_stg(df_clean, batch_id)
        rows_loaded, is_resub = loader.load_to_fact(df_clean, file_info, batch_id)

        stats["rows_loaded"] = rows_loaded
        stats["is_resubmission"] = is_resub
        stats["rows_failed"] = val_result.failed_rows
        stats["status"] = "SUCCESS"

        # Archive file
        archive_path = file_mgr.archive_file(file_path)
        audit.complete_file(
            file_log_id=file_log_id,
            status="RESUBMISSION" if is_resub else "SUCCESS",
            row_count_source=rows_source,
            row_count_loaded=rows_loaded,
            row_count_failed=val_result.failed_rows,
            is_resubmission="Y" if is_resub else "N",
            file_path_archive=str(archive_path),
            validation_errors=val_result.error_summary if val_result.errors else None
        )

    except Exception as e:
        logger.exception(f"Error processing {file_info['file_name']}: {e}")
        try:
            q_path = file_mgr.quarantine_file(file_path, str(e))
            audit.complete_file(
                file_log_id=file_log_id,
                status="FAILED",
                row_count_source=stats["rows_source"],
                file_path_quarantine=str(q_path),
                error_message=str(e)[:4000]
            )
        except Exception:
            audit.complete_file(
                file_log_id=file_log_id,
                status="FAILED",
                error_message=str(e)[:4000]
            )
        stats["status"] = "FAILED"

    return stats


def run_pipeline(
    jenkins_job: str = None,
    build_number: str = None,
    sheets: list = None
) -> int:
    """
    Run the full ETL pipeline.

    Args:
        jenkins_job: Jenkins job name for audit
        build_number: Jenkins build number for audit
        sheets: list of sheet keys to process, default both

    Returns:
        Exit code: 0 success, 1 failure
    """
    if sheets is None:
        sheets = ["savings_results", "savings_forecast"]

    sheet_type_map = {
        "savings_results":  "ACTUALS",
        "savings_forecast": "FORECAST"
    }

    audit = AuditLogger(db)
    notifier = Notifier()
    file_mgr = FileManager()

    # Start batch
    batch_id = audit.start_batch(jenkins_job, build_number)

    # Aggregate stats
    agg = {
        "files_expected":   0,
        "files_succeeded":  0,
        "files_failed":     0,
        "files_quarantined":0,
        "rows_loaded":      0,
        "rows_failed":      0
    }

    try:
        # Scan for files
        files = file_mgr.scan_landing_zone()
        agg["files_expected"] = len(files)

        if not files:
            logger.warning("No files found in landing zone")

        # Process each file for each sheet
        for file_info in files:
            file_success = True
            for sheet_key in sheets:
                sheet_type = sheet_type_map[sheet_key]
                logger.info(
                    f"Processing {file_info['file_name']} sheet={sheet_key}"
                )

                # Note: file moves after first sheet, so re-resolve path
                # For multi-sheet files, process both before archiving
                # Simplified here: process actuals then forecast from same file
                stats = process_file(
                    file_info, sheet_key, sheet_type,
                    batch_id, audit, file_mgr
                )

                agg["rows_loaded"] += stats["rows_loaded"]
                agg["rows_failed"] += stats["rows_failed"]

                if stats["status"] == "QUARANTINE":
                    agg["files_quarantined"] += 1
                    file_success = False
                    break  # Don't process forecast if actuals quarantined
                elif stats["status"] == "FAILED":
                    file_success = False
                    break

            if file_success:
                agg["files_succeeded"] += 1
            else:
                agg["files_failed"] += 1

        # Determine batch status
        if agg["files_failed"] == 0 and agg["files_quarantined"] == 0:
            status = "SUCCESS"
        elif agg["files_succeeded"] == 0:
            status = "FAILED"
        else:
            status = "PARTIAL"

        agg["status"] = status

        audit.complete_batch(
            batch_id=batch_id,
            status=status,
            **{k: v for k, v in agg.items() if k != "status"}
        )

        notifier.notify_batch_complete(batch_id, agg)

        logger.info(f"Pipeline complete: {status}")
        return 0 if status in ("SUCCESS", "PARTIAL") else 1

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        agg["status"] = "FAILED"
        audit.complete_batch(
            batch_id=batch_id,
            status="FAILED",
            error_message=str(e)[:4000],
            **{k: v for k, v in agg.items() if k != "status"}
        )
        notifier.notify_batch_complete(batch_id, agg)
        return 1

    finally:
        db.close_pool()


def main():
    parser = argparse.ArgumentParser(description="CES Savings ETL Pipeline")
    parser.add_argument("--jenkins-job", help="Jenkins job name", default=None)
    parser.add_argument("--build-number", help="Jenkins build number", default=None)
    parser.add_argument(
        "--sheets",
        nargs="+",
        choices=["savings_results", "savings_forecast"],
        help="Sheets to process (default both)",
        default=None
    )
    args = parser.parse_args()

    exit_code = run_pipeline(
        jenkins_job=args.jenkins_job,
        build_number=args.build_number,
        sheets=args.sheets
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
