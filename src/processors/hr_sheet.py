import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from src.config import settings
from src.utils.gsheets_utils import get_gsheets_service, read_sheet_values
from src.utils.gdrive_utils import (
    get_gdrive_service,
    upload_file_to_gdrive,
    delete_files_in_folder,
)

logger = logging.getLogger(__name__)


def normalize_header(header: str) -> str:
    return header.strip().lower()


def parse_hr_rows(values: List[List[str]]) -> List[Dict[str, Any]]:
    if not values:
        return []
    headers = [normalize_header(h) for h in values[0]]
    data_rows = []
    for row in values[1:]:
        entry: Dict[str, Any] = {}
        for idx, col in enumerate(row):
            if idx >= len(headers):
                continue
            key = headers[idx]
            entry[key] = col.strip()
        data_rows.append(entry)
    return data_rows


def extract_knowledge(entries: List[Dict[str, Any]]) -> str:
    # Expected key names from provided sample (normalized to lowercase)
    # We'll fetch a subset crucial for hiring, probation, transitions, terminations
    lines: List[str] = []
    lines.append("---")
    lines.append("source: HR Google Sheet")
    lines.append("category: People Lifecycle")
    lines.append("data_format: 'structured_bulleted_text'")
    lines.append("generated_at: " + datetime.utcnow().isoformat())
    lines.append("---\n")

    for e in entries:
        name = e.get("имя", "").strip() or e.get("name", "").strip()
        status = e.get("статус", "").strip().lower()
        direction = e.get("направление", "").strip()
        teamlead = e.get("тимлид", "").strip()
        current_position = (
            e.get("текущая позиция", "").strip() or e.get("позиция", "").strip()
        )
        start_date = (
            e.get("дата начала работы", "").strip() or e.get("начало ис", "").strip()
        )
        probation_end = e.get("конец ис", "").strip()
        probation_passed = e.get("ис пройден", "").strip()
        transition_dates = [
            ("intern", e.get("переход на intern", "").strip()),
            ("junior", e.get("переход на junior", "").strip()),
            ("middle", e.get("переход на middle", "").strip()),
            ("senior", e.get("переход на senior", "").strip()),
        ]
        core_date = e.get("дата перехода в core", "").strip()
        termination_status = e.get("увольнение", "").strip()
        termination_reason = e.get("причина увольнения", "").strip()
        termination_date = (
            e.get("дата", "").strip() or e.get("дата расторжения договора", "").strip()
        )

        if not name:
            continue

        lines.append(f"- Person: {name}")
        if current_position:
            lines.append(f"  - Current position: {current_position}")
        if direction:
            lines.append(f"  - Direction: {direction}")
        if teamlead:
            lines.append(f"  - Team lead: {teamlead}")
        if start_date:
            lines.append(f"  - Start date: {start_date}")
        if probation_end:
            lines.append(
                f"  - Probation end: {probation_end} (passed: {probation_passed or 'unknown'})"
            )

        # Career transitions
        promoted = [(lvl, dt) for lvl, dt in transition_dates if dt]
        if promoted:
            lines.append("  - Promotions:")
            for lvl, dt in promoted:
                lines.append(f"    - {lvl}: {dt}")
        if core_date:
            lines.append(f"  - Moved to Core: {core_date}")

        # Termination
        if termination_status:
            lines.append("  - Termination:")
            if termination_date:
                lines.append(f"    - date: {termination_date}")
            if termination_reason:
                lines.append(f"    - reason: {termination_reason}")
        lines.append("")

    return "\n".join(lines)


def process_hr_sheet() -> None:
    if (
        not settings.google_sheets_hr_spreadsheet_id
        or not settings.google_drive_hr_processed_dir_id
    ):
        logger.warning("HR sheet settings are not configured; skipping HR processor.")
        return

    gsheets = get_gsheets_service()
    if not gsheets:
        logger.error("Could not get Google Sheets service. Aborting HR processing.")
        return

    gdrive = get_gdrive_service()
    if not gdrive:
        logger.error("Could not get Google Drive service. Aborting HR processing.")
        return

    logger.info(
        f"Clearing processed folder ID: {settings.google_drive_hr_processed_dir_id}..."
    )
    delete_files_in_folder(gdrive, settings.google_drive_hr_processed_dir_id)
    logger.info("Processed folder cleared.")

    sheet_name = settings.google_sheets_hr_sheet_name or "Sheet1"
    range_a1 = f"{sheet_name}!{settings.google_sheets_hr_range}"

    logger.info(
        f"Reading HR sheet '{sheet_name}' (range {settings.google_sheets_hr_range}) from spreadsheet {settings.google_sheets_hr_spreadsheet_id}"
    )

    values = read_sheet_values(
        gsheets, settings.google_sheets_hr_spreadsheet_id, range_a1
    )
    if not values:
        logger.info("No data returned from HR sheet.")
        return

    # Skip the first row (non-valid data), use the second row as headers
    if len(values) <= 1:
        logger.info("HR sheet does not have enough rows after skipping the first row.")
        return
    entries = parse_hr_rows(values[1:])
    knowledge_text = extract_knowledge(entries)

    output_name = "hr_people_knowledge.docx"
    output_path: Path = settings.temp_dir / output_name
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(knowledge_text)
        logger.info(f"Generated HR knowledge file: {output_path}")

        logger.info(f"Uploading {output_path.name} to Google Drive as a Google Doc...")
        upload_file_to_gdrive(
            gdrive, output_path, settings.google_drive_hr_processed_dir_id, as_gdoc=True
        )
    except OSError as e:
        logger.error(f"Error writing HR knowledge file {output_path}: {e}")
