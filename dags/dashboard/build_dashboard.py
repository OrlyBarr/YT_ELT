# Builds a self-contained HTML dashboard from core.yt_api.
# Runs as the last task of the data_quality DAG, so the file is refreshed every time
# validated data lands in the DWH. Can also be run by hand inside the worker:
#   docker exec airflow-worker python /opt/airflow/dags/dashboard/build_dashboard.py
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

TEMPLATE_PATH = Path(__file__).with_name("template.html")
OUTPUT_PATH = Path("./data/dashboard/yt_dashboard.html")  # relative to /opt/airflow
DATA_PLACEHOLDER = "/*__DASHBOARD_DATA__*/null"
DATA_DIR = Path("./data")  # where produce_json writes YT_data_<date>.json
SAMPLE_COUNT = 8  # newest N daily extracts shown on the dashboard; None = all of them
SAMPLE_FILE_RE = re.compile(r"^YT_data_(\d{4}-\d{2}-\d{2})\.json$")

CORE_QUERY = """
    SELECT "Video_ID",
           "Video_Title",
           "Upload_Date",
           EXTRACT(EPOCH FROM "Duration")::INT AS duration_s,
           "Video_Type",
           COALESCE("Video_Views", 0)    AS views,
           COALESCE("Likes_Count", 0)    AS likes,
           COALESCE("Comments_Count", 0) AS comments
    FROM core.yt_api
    ORDER BY "Upload_Date";
"""


def fetch_core_rows(cur):
    """Return core.yt_api rows as dicts (cur is a RealDictCursor)."""
    cur.execute(CORE_QUERY)
    return cur.fetchall()


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0  # e.g. likeCount is missing when a video hides its likes


def summarize_sample(sample_date, videos):
    """Channel totals for one daily extract (the raw JSON written by produce_json)."""
    return {
        "date": sample_date.isoformat(),
        "ids": {v["video_id"] for v in videos},
        "views": sum(_to_int(v.get("viewCount")) for v in videos),
        "likes": sum(_to_int(v.get("likeCount")) for v in videos),
        "comments": sum(_to_int(v.get("commentCount")) for v in videos),
    }


def load_samples(data_dir=DATA_DIR, count=SAMPLE_COUNT):
    """Return the daily samples (all of them, or the newest `count`) with the change
    since the sample before each.

    core.yt_api only keeps the latest values, so history comes from the dated JSON
    extracts. When `count` is set, one extra older file is read so the oldest returned
    sample has a delta too.
    """
    files = []
    for path in Path(data_dir).glob("YT_data_*.json"):
        match = SAMPLE_FILE_RE.match(path.name)
        if match:
            files.append((date.fromisoformat(match.group(1)), path))
    files.sort()

    summaries = []
    if count is not None:
        files = files[-(count + 1):]

    for sample_date, path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                summaries.append(summarize_sample(sample_date, json.load(f)))
        except (OSError, ValueError, KeyError, TypeError) as e:
            logger.warning(f"Skipping unreadable sample {path.name}: {e}")

    samples = []
    for i, cur in enumerate(summaries):
        prev = summaries[i - 1] if i > 0 else None
        samples.append({
            "date": cur["date"],
            "videos": len(cur["ids"]),
            "views": cur["views"],
            "likes": cur["likes"],
            "comments": cur["comments"],
            "prev_date": prev["date"] if prev else None,
            "prev_views": prev["views"] if prev else None,
            "prev_likes": prev["likes"] if prev else None,
            "new_videos": len(cur["ids"] - prev["ids"]) if prev else None,
            "removed_videos": len(prev["ids"] - cur["ids"]) if prev else None,
        })
    return samples if count is None else samples[-count:]


def to_payload(rows, generated_at, samples=None):
    """Compact the rows into the JSON shape the template expects."""
    videos = [
        [
            r["Video_ID"],
            r["Video_Title"],
            r["Upload_Date"].strftime("%Y-%m-%dT%H:%M:%S"),
            int(r["duration_s"]),
            r["Video_Type"],
            int(r["views"]),
            int(r["likes"]),
            int(r["comments"]),
        ]
        for r in rows
    ]
    return {
        "generated_at": generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "columns": ["id", "title", "uploaded", "duration_s", "type", "views", "likes", "comments"],
        "videos": videos,
        "samples": samples or [],
    }


def render_dashboard(rows, generated_at=None, samples=None):
    generated_at = generated_at or datetime.now(timezone.utc)
    payload = json.dumps(to_payload(rows, generated_at, samples), ensure_ascii=False, separators=(",", ":"))
    # Keep "</script>" inside a title from closing the script tag.
    payload = payload.replace("</", "<\/")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    if DATA_PLACEHOLDER not in template:
        raise ValueError(f"Placeholder {DATA_PLACEHOLDER!r} not found in {TEMPLATE_PATH}")
    return template.replace(DATA_PLACEHOLDER, payload)


def write_dashboard(html, output_path=OUTPUT_PATH):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(".tmp")
    tmp_path.write_text(html, encoding="utf-8")
    # Atomic swap so a browser auto-refresh never reads a half-written file.
    os.replace(tmp_path, output_path)
    return output_path


def generate(output_path=OUTPUT_PATH):
    from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor

    conn, cur = get_conn_cursor()
    try:
        rows = fetch_core_rows(cur)
    finally:
        close_conn_cursor(conn, cur)

    samples = load_samples()
    path = write_dashboard(render_dashboard(rows, samples=samples), output_path)
    logger.info(f"Dashboard written to {path} with {len(rows)} videos and {len(samples)} samples")
    return str(path)


def build_dashboard_task():
    from airflow.decorators import task

    @task(task_id="build_dashboard")
    def build_dashboard():
        return generate()

    return build_dashboard()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # make dags/ importable
    print(generate())
