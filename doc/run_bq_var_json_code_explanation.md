# run_bq_var_json.py 라인별 상세 주석/설명

이 문서는 [py/run_bq_var_json.py](../py/run_bq_var_json.py) 소스(현재 시점)를 **라인 단위로** 설명합니다.

목표: 코드를 한 줄씩 따라가며 “왜 이 줄이 필요한지 / 무엇을 하는지”를 스스로 이해하고 수정할 수 있게 만드는 것입니다.

---

## 원본 코드 (참고용)

아래는 현재 소스의 전체 코드입니다(라인 번호는 아래 “라인별 주석” 섹션과 매칭됩니다).

```python
001: #!/usr/bin/env python3
002: import csv
003: import json
004: import logging
005: import os
006: import subprocess
007: import sys
008: from datetime import datetime
009: from pathlib import Path
010: from typing import Dict, List, Tuple
011: 
012: 
013: logger = logging.getLogger(__name__)
014: 
015: 
016: # ============================
017: # Config
018: # ============================
019: class Config:
020:     BASE_DIR = Path(__file__).resolve().parents[1]
021:     SQL_DIR = BASE_DIR / "sql"
022:     CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"
023:     JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"
024: 
025: 
026: # ============================
027: # Logging
028: # ============================
029: class _MaxLevelFilter(logging.Filter):
030:     def __init__(self, max_level: int):
031:         super().__init__()
032:         self._max_level = max_level
033: 
034:     def filter(self, record: logging.LogRecord) -> bool:
035:         return record.levelno <= self._max_level
036: 
037: 
038: def setup_logging(base_dir: Path) -> Tuple[Path, Path]:
039:     run_date = datetime.now().strftime("%Y%m%d")
040:     log_dir = base_dir / "log" / run_date
041:     log_dir.mkdir(parents=True, exist_ok=True)
042: 
043:     stamp = datetime.now().strftime("%H%M%S")
044:     base = "run_bq_var_json.{}.{}".format(stamp, os.getpid())
045: 
046:     out_log = log_dir / (base + ".log")
047:     err_log = log_dir / (base + ".log.err")
048: 
049:     fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
050:     root = logging.getLogger()
051:     root.setLevel(logging.INFO)
052: 
053:     for h in list(root.handlers):
054:         root.removeHandler(h)
055: 
056:     console = logging.StreamHandler()
057:     console.setLevel(logging.INFO)
058:     console.setFormatter(fmt)
059: 
060:     file_out = logging.FileHandler(out_log, encoding="utf-8")
061:     file_out.setLevel(logging.INFO)
062:     file_out.setFormatter(fmt)
063:     file_out.addFilter(_MaxLevelFilter(logging.WARNING))
064: 
065:     file_err = logging.FileHandler(err_log, encoding="utf-8")
066:     file_err.setLevel(logging.ERROR)
067:     file_err.setFormatter(fmt)
068: 
069:     root.addHandler(console)
070:     root.addHandler(file_out)
071:     root.addHandler(file_err)
072: 
073:     return out_log, err_log
074: 
075: 
076: # ============================
077: # CSV/JSON handling
078: # ============================
079: def read_csv_records(csv_path: Path) -> List[Dict[str, str]]:
080:     """Read CSV and return list of record dicts."""
081:     if not csv_path.exists():
082:         raise FileNotFoundError("CSV file not found: {}".format(csv_path))
083: 
084:     text = csv_path.read_text(encoding="utf-8", errors="replace")
085:     text = text.lstrip("\ufeff")  # Strip BOM
086: 
087:     reader = csv.DictReader(text.splitlines())
088:     records = []
089: 
090:     for row in reader:
091:         # Skip empty/comment rows
092:         if not row or not any(row.values()):
093:             continue
094:         first_val = next(iter(row.values()), "")
095:         if first_val.strip().startswith("#"):
096:             continue
097: 
098:         # Strip all values
099:         records.append({k: v.strip() for k, v in row.items() if k})
100: 
101:     return records
102: 
103: 
104: def save_json(json_path: Path, records: List[Dict[str, str]]) -> None:
105:     """Write records to JSON file."""
106:     json_path.parent.mkdir(parents=True, exist_ok=True)
107:     json_path.write_text(
108:         json.dumps(records, ensure_ascii=False, indent=2) + "\n",
109:         encoding="utf-8"
110:     )
111: 
112: 
113: # ============================
114: # CLI parsing
115: # ============================
116: def parse_cli_args(argv: List[str]) -> Dict[str, str]:
117:     """Parse key=value arguments."""
118:     args = {}
119:     for token in argv:
120:         if "=" not in token:
121:             raise ValueError("Invalid arg (expected key=value): {}".format(token))
122:         key, value = token.split("=", 1)
123:         args[key.strip()] = value.strip()
124:     return args
125: 
126: 
127: # ============================
128: # BigQuery execution
129: # ============================
130: def quote_bq_string(value: str) -> str:
131:     """Escape single quotes for BigQuery."""
132:     return "'" + value.replace("'", "''") + "'"
133: 
134: 
135: def substitute_sql(template: str, pgm_id: str, job_dt: str, tbl_id: str) -> str:
136:     """Replace placeholders with quoted values."""
137:     return (
138:         template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))
139:         .replace("{vs_job_dt}", quote_bq_string(job_dt))
140:         .replace("{vs_tbl_id}", quote_bq_string(tbl_id))
141:     )
142: 
143: 
144: def run_bq_query(sql_text: str) -> None:
145:     """Execute BigQuery query."""
146:     subprocess.run(
147:         ["bq", "query", "--quiet", "--use_legacy_sql=false"],
148:         input=sql_text,
149:         universal_newlines=True,
150:         check=True,
151:     )
152: 
153: 
154: # ============================
155: # Core logic
156: # ============================
157: def apply_filters(records: List[Dict[str, str]], cli_args: Dict[str, str]) -> List[Dict[str, str]]:
158:     """Filter records by use_yn=Y and CLI filters (mid, vs_pgm_id)."""
159:     # Filter: use_yn=Y (or missing)
160:     targets = [r for r in records if r.get("use_yn", "Y").strip().upper() == "Y"]
161: 
162:     # CLI filter: mid
163:     if "mid" in cli_args:
164:         targets = [r for r in targets if r.get("mid") == cli_args["mid"]]
165: 
166:     # CLI filter: vs_pgm_id
167:     if "vs_pgm_id" in cli_args:
168:         targets = [r for r in targets if r.get("vs_pgm_id") == cli_args["vs_pgm_id"]]
169: 
170:     return targets
171: 
172: 
173: def execute_sql_jobs(targets: List[Dict[str, str]], overrides: Dict[str, str]) -> Tuple[int, int, int]:
174:     """Execute SQL for each target record. Returns (total, success, fail)."""
175:     total = success = fail = 0
176: 
177:     for record in targets:
178:         # Apply overrides
179:         effective = dict(record)
180:         effective.update(overrides)
181: 
182:         vs_pgm_id = effective.get("vs_pgm_id", "").strip()
183:         vs_job_dt = effective.get("vs_job_dt", "").strip()
184:         vs_tbl_id = effective.get("vs_tbl_id", "").strip()
185: 
186:         if not vs_pgm_id:
187:             logger.error("Missing vs_pgm_id in record: %s", effective)
188:             fail += 1
189:             continue
190: 
191:         # Resolve SQL file path
192:         sql_path = Config.SQL_DIR / vs_pgm_id
193:         if not sql_path.exists():
194:             logger.error("SQL file not found: %s", sql_path)
195:             fail += 1
196:             continue
197: 
198:         # Execute
199:         total += 1
200:         logger.info(
201:             "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s)",
202:             vs_pgm_id,
203:             effective.get("mid", ""),
204:             vs_job_dt,
205:             vs_tbl_id,
206:         )
207: 
208:         try:
209:             pgm_id = sql_path.stem
210:             template = sql_path.read_text(encoding="utf-8", errors="replace")
211:             sql_text = substitute_sql(template, pgm_id, vs_job_dt, vs_tbl_id)
212:             run_bq_query(sql_text)
213:             success += 1
214:         except subprocess.CalledProcessError as e:
215:             logger.error("bq query failed (exit_code=%s)", e.returncode)
216:             fail += 1
217: 
218:     return total, success, fail
219: 
220: 
221: # ============================
222: # Entry Point
223: # ============================
224: def main() -> int:
225:     out_log, err_log = setup_logging(Config.BASE_DIR)
226:     logger.info("SUCCESS LOG : %s", out_log)
227:     logger.info("ERROR LOG   : %s", err_log)
228: 
229:     # Parse CLI arguments
230:     try:
231:         cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
232:     except ValueError as e:
233:         logger.error("%s", e)
234:         logger.error(
235:             "Usage: python %s [mid=<mid>] [vs_pgm_id=<file.sql>] [vs_job_dt=<yyyymmdd>]",
236:             sys.argv[0]
237:         )
238:         return 1
239: 
240:     # Read CSV and generate JSON baseline
241:     try:
242:         records = read_csv_records(Config.CSV_PATH)
243:         save_json(Config.JSON_PATH, records)
244:         logger.info("Generated JSON baseline: %s", Config.JSON_PATH)
245:     except Exception as e:
246:         logger.error("%s", e)
247:         return 1
248: 
249:     # Filter records
250:     targets = apply_filters(records, cli_args)
251:     if not targets:
252:         logger.error("No target rows matched the given filters (and use_yn=Y).")
253:         return 1
254: 
255:     # Extract overrides (non-filter CLI args)
256:     overrides = {k: v for k, v in cli_args.items() if k not in ("mid", "vs_pgm_id")}
257: 
258:     # Execute SQL jobs
259:     total, success, fail = execute_sql_jobs(targets, overrides)
260: 
261:     logger.info("SUMMARY total=%s, success=%s, fail=%s", total, success, fail)
262:     return 0 if fail == 0 else 1
263: 
264: 
265: if __name__ == "__main__":
266:     raise SystemExit(main())
```

---

## 라인별 주석 (L001 ~ L266)

형식: `L번호: 코드` — 설명

- L001: `#!/usr/bin/env python3` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L002: `import csv` — CSV 파일 파싱에 사용합니다(헤더 기반 dict 변환은 csv.DictReader).
- L003: `import json` — CSV→JSON 기준정보(bq.json) 저장에 사용합니다.
- L004: `import logging` — 콘솔/파일 로그 출력을 구성할 때 사용합니다.
- L005: `import os` — PID 등 OS 정보를 사용합니다(로그 파일명 중복 방지).
- L006: `import subprocess` — bq CLI 실행(bq query)을 호출할 때 사용합니다.
- L007: `import sys` — CLI 인자(sys.argv)와 종료 코드 처리를 위해 사용합니다.
- L008: `from datetime import datetime` — 현재 시각을 얻고 문자열로 포맷팅(strftime)하기 위해 datetime을 가져옵니다.
- L009: `from pathlib import Path` — 파일/디렉토리 경로를 안전하게 다루기 위해 pathlib.Path를 사용합니다.
- L010: `from typing import Dict, List, Tuple` — 타입 힌트를 위한 typing 심볼을 import 합니다: Dict, List, Tuple
- L011: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L012: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L013: `logger = logging.getLogger(__name__)` — 현재 모듈 이름(__name__)으로 logger를 만들고 이후 logger.info/error로 사용합니다.
- L014: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L015: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L016: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L017: `# Config` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L018: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L019: `class Config:` — 클래스 정의: 관련 기능/데이터를 묶는 단위를 선언합니다.
- L020: `    BASE_DIR = Path(__file__).resolve().parents[1]` — 현재 파일(py/...) 기준으로 프로젝트 루트(BASE_DIR)를 계산합니다.
- L021: `    SQL_DIR = BASE_DIR / "sql"` — 현재 파일(py/...) 기준으로 프로젝트 루트(BASE_DIR)를 계산합니다.
- L022: `    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"` — 현재 파일(py/...) 기준으로 프로젝트 루트(BASE_DIR)를 계산합니다.
- L023: `    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"` — 현재 파일(py/...) 기준으로 프로젝트 루트(BASE_DIR)를 계산합니다.
- L024: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L025: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L026: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L027: `# Logging` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L028: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L029: `class _MaxLevelFilter(logging.Filter):` — 클래스 정의: 관련 기능/데이터를 묶는 단위를 선언합니다.
- L030: `    def __init__(self, max_level: int):` — 필터 초기화: 통과시킬 최대 로그 레벨(max_level)을 입력으로 받습니다.
- L031: `        super().__init__()` — 부모 클래스(logging.Filter) 초기화를 호출합니다.
- L032: `        self._max_level = max_level` — 인자로 받은 max_level을 인스턴스 변수로 저장합니다.
- L033: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L034: `    def filter(self, record: logging.LogRecord) -> bool:` — 필터 함수: 각 로그 레코드(record)가 통과할지(True/False) 결정합니다.
- L035: `        return record.levelno <= self._max_level` — 레코드의 레벨 번호가 최대 허용 레벨 이하이면 통과시킵니다.
- L036: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L037: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L038: `def setup_logging(base_dir: Path) -> Tuple[Path, Path]:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L039: `    run_date = datetime.now().strftime("%Y%m%d")` — 로그 폴더명용 날짜(YYYYMMDD)를 생성합니다.
- L040: `    log_dir = base_dir / "log" / run_date` — 로그 디렉토리 경로(BASE_DIR/log/YYYYMMDD)를 구성합니다.
- L041: `    log_dir.mkdir(parents=True, exist_ok=True)` — 로그 디렉토리가 없으면 생성합니다(parents=True로 상위도 함께 생성).
- L042: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L043: `    stamp = datetime.now().strftime("%H%M%S")` — 로그 파일명에 넣을 시각(HHMMSS)을 생성합니다.
- L044: `    base = "run_bq_var_json.{}.{}".format(stamp, os.getpid())` — 프로세스 ID를 파일명에 넣어 동시 실행 시 로그 파일이 겹치지 않게 합니다.
- L045: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L046: `    out_log = log_dir / (base + ".log")` — INFO/WARNING 등 일반 로그를 저장할 .log 파일 경로를 만듭니다.
- L047: `    err_log = log_dir / (base + ".log.err")` — ERROR 이상만 저장할 .log.err 파일 경로를 만듭니다.
- L048: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L049: `    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")` — 로그 한 줄 출력 형식(시간, 레벨, 메시지)을 정의합니다.
- L050: `    root = logging.getLogger()` — 루트 로거를 가져와 핸들러를 연결할 준비를 합니다.
- L051: `    root.setLevel(logging.INFO)` — 루트 로거의 최소 로그 레벨을 설정합니다(logging.INFO).
- L052: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L053: `    for h in list(root.handlers):` — 기존 핸들러 목록을 복사(list)한 뒤 순회하며 제거합니다(중복 출력 방지).
- L054: `        root.removeHandler(h)` — 기존 핸들러를 제거해 같은 로그가 여러 번 찍히는 것을 방지합니다.
- L055: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L056: `    console = logging.StreamHandler()` — 콘솔(표준 출력)로 로그를 보내는 StreamHandler를 생성합니다.
- L057: `    console.setLevel(logging.INFO)` — console 핸들러의 최소 로그 레벨을 설정합니다(logging.INFO).
- L058: `    console.setFormatter(fmt)` — console가 출력할 로그 포맷(시간/레벨/메시지 형식)을 지정합니다.
- L059: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L060: `    file_out = logging.FileHandler(out_log, encoding="utf-8")` — 일반 로그용 FileHandler를 생성(out_log에 기록, utf-8 인코딩).
- L061: `    file_out.setLevel(logging.INFO)` — file_out 핸들러의 최소 로그 레벨을 설정합니다(logging.INFO).
- L062: `    file_out.setFormatter(fmt)` — file_out가 출력할 로그 포맷(시간/레벨/메시지 형식)을 지정합니다.
- L063: `    file_out.addFilter(_MaxLevelFilter(logging.WARNING))` — WARNING 이하만 out_log에 기록하도록 필터를 적용합니다(ERROR는 err_log로 분리).
- L064: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L065: `    file_err = logging.FileHandler(err_log, encoding="utf-8")` — 에러 로그용 FileHandler를 생성(err_log에 ERROR 이상 기록).
- L066: `    file_err.setLevel(logging.ERROR)` — file_err 핸들러의 최소 로그 레벨을 설정합니다(logging.ERROR).
- L067: `    file_err.setFormatter(fmt)` — file_err가 출력할 로그 포맷(시간/레벨/메시지 형식)을 지정합니다.
- L068: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L069: `    root.addHandler(console)` — 생성한 핸들러를 루트 로거에 연결해 로그가 실제로 출력되게 합니다.
- L070: `    root.addHandler(file_out)` — 생성한 핸들러를 루트 로거에 연결해 로그가 실제로 출력되게 합니다.
- L071: `    root.addHandler(file_err)` — 생성한 핸들러를 루트 로거에 연결해 로그가 실제로 출력되게 합니다.
- L072: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L073: `    return out_log, err_log` — 생성한 로그 파일 경로를 반환해 main()에서 사용자에게 안내합니다.
- L074: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L075: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L076: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L077: `# CSV/JSON handling` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L078: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L079: `def read_csv_records(csv_path: Path) -> List[Dict[str, str]]:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L080: `    """Read CSV and return list of record dicts."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L081: `    if not csv_path.exists():` — 입력 CSV 파일 존재 여부를 검사합니다.
- L082: `        raise FileNotFoundError("CSV file not found: {}".format(csv_path))` — 필수 입력 파일이 없으므로 예외를 발생시켜 즉시 중단합니다(원인 파악 용이).
- L083: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L084: `    text = csv_path.read_text(encoding="utf-8", errors="replace")` — CSV 파일을 텍스트로 통째로 읽습니다(utf-8, 깨진 문자는 대체).
- L085: `    text = text.lstrip("\ufeff")  # Strip BOM` — UTF-8 BOM(﻿)을 제거해 헤더가 깨지는 문제를 방지합니다.
- L086: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L087: `    reader = csv.DictReader(text.splitlines())` — 첫 줄 헤더를 기준으로 각 행을 dict로 파싱하는 DictReader를 생성합니다.
- L088: `    records = []` — 파싱 결과를 담을 리스트(records)를 초기화합니다.
- L089: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L090: `    for row in reader:` — CSV의 각 행(row)을 순회하면서 레코드를 쌓습니다.
- L091: `        # Skip empty/comment rows` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L092: `        if not row or not any(row.values()):` — 완전히 비어있는 행(공백/빈 컬럼)이라면 건너뜁니다.
- L093: `            continue` — 현재 반복(iteration)을 중단하고 다음 항목으로 넘어갑니다.
- L094: `        first_val = next(iter(row.values()), "")` — 첫 번째 컬럼 값을 가져와 주석 행(#...) 여부를 판단합니다.
- L095: `        if first_val.strip().startswith("#"):` — 첫 값이 #으로 시작하면 주석 행으로 보고 건너뜁니다.
- L096: `            continue` — 현재 반복(iteration)을 중단하고 다음 항목으로 넘어갑니다.
- L097: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L098: `        # Strip all values` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L099: `        records.append({k: v.strip() for k, v in row.items() if k})` — 각 컬럼 값을 strip()하여 공백을 제거한 뒤 records에 추가합니다.
- L100: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L101: `    return records` — 최종 레코드 리스트를 반환합니다.
- L102: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L103: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L104: `def save_json(json_path: Path, records: List[Dict[str, str]]) -> None:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L105: `    """Write records to JSON file."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L106: `    json_path.parent.mkdir(parents=True, exist_ok=True)` — JSON 파일의 상위 폴더가 없으면 생성합니다.
- L107: `    json_path.write_text(` — 생성된 JSON 문자열을 파일로 저장합니다.
- L108: `        json.dumps(records, ensure_ascii=False, indent=2) + "\n",` — 레코드 리스트를 JSON 문자열로 변환합니다(한글 유지, 보기 좋게 들여쓰기).
- L109: `        encoding="utf-8"` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L110: `    )` — 이전 줄에서 시작한 호출/정의의 괄호를 닫습니다(문법적 마무리).
- L111: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L112: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L113: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L114: `# CLI parsing` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L115: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L116: `def parse_cli_args(argv: List[str]) -> Dict[str, str]:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L117: `    """Parse key=value arguments."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L118: `    args = {}` — 파싱 결과를 담을 dict(args)를 초기화합니다.
- L119: `    for token in argv:` — 각 CLI 인자(token)를 순회합니다(예: mid=qa).
- L120: `        if "=" not in token:` — key=value 형태가 아니라면 사용자 입력 오류로 간주합니다.
- L121: `            raise ValueError("Invalid arg (expected key=value): {}".format(token))` — 예외를 발생시켜 호출자에게 오류를 알립니다.
- L122: `        key, value = token.split("=", 1)` — 첫 번째 = 기준으로 key/value를 분리합니다(값에 =가 있어도 안전).
- L123: `        args[key.strip()] = value.strip()` — key/value 양쪽 공백을 제거한 뒤 dict에 저장합니다.
- L124: `    return args` — 파싱된 CLI 인자 dict를 반환합니다.
- L125: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L126: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L127: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L128: `# BigQuery execution` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L129: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L130: `def quote_bq_string(value: str) -> str:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L131: `    """Escape single quotes for BigQuery."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L132: `    return "'" + value.replace("'", "''") + "'"` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L133: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L134: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L135: `def substitute_sql(template: str, pgm_id: str, job_dt: str, tbl_id: str) -> str:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L136: `    """Replace placeholders with quoted values."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L137: `    return (` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L138: `        template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L139: `        .replace("{vs_job_dt}", quote_bq_string(job_dt))` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L140: `        .replace("{vs_tbl_id}", quote_bq_string(tbl_id))` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L141: `    )` — 이전 줄에서 시작한 호출/정의의 괄호를 닫습니다(문법적 마무리).
- L142: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L143: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L144: `def run_bq_query(sql_text: str) -> None:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L145: `    """Execute BigQuery query."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L146: `    subprocess.run(` — bq query를 실행합니다(check=True로 실패 시 예외 발생).
- L147: `        ["bq", "query", "--quiet", "--use_legacy_sql=false"],` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L148: `        input=sql_text,` — SQL 텍스트를 표준 입력(STDIN)으로 bq에 전달합니다.
- L149: `        universal_newlines=True,` — 입출력을 텍스트 모드로 처리합니다(Python 3에서 문자열로 다룸).
- L150: `        check=True,` — 명령이 실패(비정상 종료 코드)하면 CalledProcessError 예외를 발생시킵니다.
- L151: `    )` — 이전 줄에서 시작한 호출/정의의 괄호를 닫습니다(문법적 마무리).
- L152: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L153: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L154: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L155: `# Core logic` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L156: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L157: `def apply_filters(records: List[Dict[str, str]], cli_args: Dict[str, str]) -> List[Dict[str, str]]:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L158: `    """Filter records by use_yn=Y and CLI filters (mid, vs_pgm_id)."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L159: `    # Filter: use_yn=Y (or missing)` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L160: `    targets = [r for r in records if r.get("use_yn", "Y").strip().upper() == "Y"]` — use_yn이 Y(또는 누락)인 레코드만 실행 대상으로 필터링합니다.
- L161: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L162: `    # CLI filter: mid` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L163: `    if "mid" in cli_args:` — CLI에 mid가 있으면 해당 mid만 남기도록 추가 필터를 적용합니다.
- L164: `        targets = [r for r in targets if r.get("mid") == cli_args["mid"]]` — mid 값이 일치하는 레코드만 남깁니다.
- L165: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L166: `    # CLI filter: vs_pgm_id` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L167: `    if "vs_pgm_id" in cli_args:` — CLI에 vs_pgm_id가 있으면 해당 프로그램/SQL만 실행하도록 필터합니다.
- L168: `        targets = [r for r in targets if r.get("vs_pgm_id") == cli_args["vs_pgm_id"]]` — vs_pgm_id 값이 일치하는 레코드만 남깁니다.
- L169: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L170: `    return targets` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L171: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L172: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L173: `def execute_sql_jobs(targets: List[Dict[str, str]], overrides: Dict[str, str]) -> Tuple[int, int, int]:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L174: `    """Execute SQL for each target record. Returns (total, success, fail)."""` — Docstring(문서 문자열): 이 함수가 무엇을 하는지 한 줄로 설명합니다.
- L175: `    total = success = fail = 0` — 총 실행 건수/성공/실패 카운터를 0으로 초기화합니다.
- L176: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L177: `    for record in targets:` — 필터링된 대상 레코드를 하나씩 처리합니다(레코드 1개 = SQL 1회 실행 시도).
- L178: `        # Apply overrides` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L179: `        effective = dict(record)` — 원본 레코드를 보존하기 위해 복사본(effective)을 만듭니다.
- L180: `        effective.update(overrides)` — CLI 오버라이드(vs_job_dt, vs_tbl_id 등)를 복사본에 덮어씌웁니다.
- L181: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L182: `        vs_pgm_id = effective.get("vs_pgm_id", "").strip()` — 실행할 SQL 파일명(vs_pgm_id)을 가져오고 앞뒤 공백을 제거합니다.
- L183: `        vs_job_dt = effective.get("vs_job_dt", "").strip()` — SQL 템플릿 치환에 사용할 작업일자(vs_job_dt)를 가져옵니다.
- L184: `        vs_tbl_id = effective.get("vs_tbl_id", "").strip()` — SQL 템플릿 치환에 사용할 테이블ID(vs_tbl_id)를 가져옵니다.
- L185: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L186: `        if not vs_pgm_id:` — SQL 파일명이 없으면 실행할 수 없으므로 실패로 기록하고 다음 레코드로 넘어갑니다.
- L187: `            logger.error("Missing vs_pgm_id in record: %s", effective)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L188: `            fail += 1` — 실패 카운트를 증가합니다.
- L189: `            continue` — 현재 반복(iteration)을 중단하고 다음 항목으로 넘어갑니다.
- L190: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L191: `        # Resolve SQL file path` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L192: `        sql_path = Config.SQL_DIR / vs_pgm_id` — sql/ 디렉토리와 vs_pgm_id를 결합해 실제 SQL 파일 경로를 만듭니다.
- L193: `        if not sql_path.exists():` — SQL 파일이 실제로 존재하는지 확인합니다(없으면 실패).
- L194: `            logger.error("SQL file not found: %s", sql_path)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L195: `            fail += 1` — 실패 카운트를 증가합니다.
- L196: `            continue` — 현재 반복(iteration)을 중단하고 다음 항목으로 넘어갑니다.
- L197: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L198: `        # Execute` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L199: `        total += 1` — 이 시점부터 “실행 시도”로 카운트합니다(파일이 존재하는 레코드).
- L200: `        logger.info(` — 어떤 SQL을 어떤 파라미터로 실행하는지 INFO 로그로 남깁니다.
- L201: `            "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s)",` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L202: `            vs_pgm_id,` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L203: `            effective.get("mid", ""),` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L204: `            vs_job_dt,` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L205: `            vs_tbl_id,` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L206: `        )` — 이전 줄에서 시작한 호출/정의의 괄호를 닫습니다(문법적 마무리).
- L207: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L208: `        try:` — 예외가 발생할 수 있는 코드를 try 블록에서 실행합니다.
- L209: `            pgm_id = sql_path.stem` — 파일명에서 확장자(.sql)를 제거한 값을 {vs_pgm_id} 치환용으로 사용합니다.
- L210: `            template = sql_path.read_text(encoding="utf-8", errors="replace")` — SQL 파일 내용을 템플릿 문자열로 읽습니다.
- L211: `            sql_text = substitute_sql(template, pgm_id, vs_job_dt, vs_tbl_id)` — 템플릿의 {vs_*} 플레이스홀더를 실제 값으로 치환해 실행용 SQL을 만듭니다.
- L212: `            run_bq_query(sql_text)` — 치환된 SQL을 BigQuery CLI로 실행합니다.
- L213: `            success += 1` — 실행이 예외 없이 끝났으므로 성공 카운트를 증가합니다.
- L214: `        except subprocess.CalledProcessError as e:` — bq 명령이 실패했을 때(CalledProcessError) 실패로 집계합니다.
- L215: `            logger.error("bq query failed (exit_code=%s)", e.returncode)` — bq 실패 시 종료 코드를 함께 기록해 원인 분석을 돕습니다.
- L216: `            fail += 1` — 실패 카운트를 증가합니다.
- L217: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L218: `    return total, success, fail` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L219: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L220: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L221: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L222: `# Entry Point` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L223: `# ============================` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L224: `def main() -> int:` — 함수 정의: 이 줄에서 새로운 함수를 선언합니다.
- L225: `    out_log, err_log = setup_logging(Config.BASE_DIR)` — 로깅을 초기화하고 생성된 로그 파일 경로 2개(.log/.log.err)를 받습니다.
- L226: `    logger.info("SUCCESS LOG : %s", out_log)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L227: `    logger.info("ERROR LOG   : %s", err_log)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L228: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L229: `    # Parse CLI arguments` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L230: `    try:` — 예외가 발생할 수 있는 코드를 try 블록에서 실행합니다.
- L231: `        cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}` — CLI 인자를 파싱합니다(인자가 없으면 빈 dict).
- L232: `    except ValueError as e:` — CLI 인자 형식 오류(ValueError)를 잡아 사용법을 안내합니다.
- L233: `        logger.error("%s", e)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L234: `        logger.error(` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L235: `            "Usage: python %s [mid=<mid>] [vs_pgm_id=<file.sql>] [vs_job_dt=<yyyymmdd>]",` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L236: `            sys.argv[0]` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L237: `        )` — 이전 줄에서 시작한 호출/정의의 괄호를 닫습니다(문법적 마무리).
- L238: `        return 1` — 프로그램을 실패 종료 코드(1)로 끝냅니다.
- L239: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L240: `    # Read CSV and generate JSON baseline` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L241: `    try:` — 예외가 발생할 수 있는 코드를 try 블록에서 실행합니다.
- L242: `        records = read_csv_records(Config.CSV_PATH)` — CSV를 읽어 전체 레코드 목록을 만듭니다.
- L243: `        save_json(Config.JSON_PATH, records)` — 레코드 목록을 JSON 기준정보 파일로 저장합니다.
- L244: `        logger.info("Generated JSON baseline: %s", Config.JSON_PATH)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L245: `    except Exception as e:` — 모든 예외를 잡아 로그로 남기고 실패(1)로 종료하기 위한 처리입니다.
- L246: `        logger.error("%s", e)` — 이 줄은 상위/하위 줄과 함께 하나의 동작을 구성합니다(변수/함수 호출/블록 흐름을 같이 보세요).
- L247: `        return 1` — 프로그램을 실패 종료 코드(1)로 끝냅니다.
- L248: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L249: `    # Filter records` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L250: `    targets = apply_filters(records, cli_args)` — 전체 레코드에서 실행 대상만 골라냅니다(use_yn/mid/vs_pgm_id).
- L251: `    if not targets:` — 블록 시작: 들여쓰기된 하위 줄들이 이 블록에 속합니다.
- L252: `        logger.error("No target rows matched the given filters (and use_yn=Y).")` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L253: `        return 1` — 프로그램을 실패 종료 코드(1)로 끝냅니다.
- L254: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L255: `    # Extract overrides (non-filter CLI args)` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L256: `    overrides = {k: v for k, v in cli_args.items() if k not in ("mid", "vs_pgm_id")}` — mid/vs_pgm_id는 필터용이므로 제외하고 나머지를 “값 덮어쓰기(overrides)”로 분리합니다.
- L257: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L258: `    # Execute SQL jobs` — 주석: 코드의 의도/섹션을 설명하거나 가독성을 높이기 위한 구분선입니다.
- L259: `    total, success, fail = execute_sql_jobs(targets, overrides)` — 필터링된 대상에 대해 SQL 실행을 수행하고 통계를 받습니다.
- L260: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L261: `    logger.info("SUMMARY total=%s, success=%s, fail=%s", total, success, fail)` — 전체 실행 결과(total/success/fail)를 요약 로그로 출력합니다.
- L262: `    return 0 if fail == 0 else 1` — fail이 0이면 성공(0), 하나라도 있으면 실패(1)로 종료합니다.
- L263: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L264: `` — 빈 줄: 논리 블록을 나눠 읽기 쉽게 합니다.
- L265: `if __name__ == "__main__":` — 대입(assign): 오른쪽 표현식을 평가한 결과를 왼쪽 변수에 저장합니다.
- L266: `    raise SystemExit(main())` — main() 반환값을 프로세스 종료 코드로 전달하기 위해 SystemExit를 발생시킵니다.

---

## 마지막 업데이트

- 날짜: 2025-12-28

- 기준 소스: py/run_bq_var_json.py

