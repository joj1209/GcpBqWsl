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


- L001: `#!/usr/bin/env python3` — Shebang: 파일을 직접 실행할 때 python3로 실행되도록 지정합니다.
- L002: `import csv` — CSV 파일 파싱에 사용 (csv.DictReader).
- L003: `import json` — CSV 내용을 JSON 기준정보로 저장할 때 사용.
- L004: `import logging` — 콘솔/파일 로그 출력에 사용.
- L005: `import os` — 프로세스 ID(getpid) 등 OS 정보에 사용 (로그 파일명 충돌 방지).
- L006: `import subprocess` — 외부 명령(bq query) 실행에 사용.
- L007: `import sys` — CLI 인자(sys.argv) 및 종료 코드 처리에 사용.
- L008: `from datetime import datetime` — 날짜/시간 문자열 포맷팅(로그 디렉토리/파일명)에 사용.
- L009: `from pathlib import Path` — 경로 조합/파일 읽기/쓰기 등 pathlib 기반 파일 처리에 사용.
- L010: `from typing import Dict, List, Tuple` — 타입 힌트 import: Dict=타입 힌트: dict 형태의 레코드. / List=타입 힌트: 레코드 목록. / Tuple=타입 힌트: (out_log, err_log) 또는 (total, success, fail).
- L011: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L012: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L013: `logger = logging.getLogger(__name__)` — 모듈 전용 logger 생성: 이 파일에서 logger.info/error로 로그를 남깁니다.
- L014: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L015: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L016: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L017: `# Config` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L018: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L019: `class Config:` — Config 클래스 시작: 프로젝트 경로(루트, sql, csv, json)를 상수로 정의합니다.
- L020: `    BASE_DIR = Path(__file__).resolve().parents[1]` — 프로젝트 루트 경로 계산: 이 파일(py/...) 기준으로 상위 폴더를 BASE_DIR로 설정합니다.
- L021: `    SQL_DIR = BASE_DIR / "sql"` — 프로젝트 루트 경로 계산: 이 파일(py/...) 기준으로 상위 폴더를 BASE_DIR로 설정합니다.
- L022: `    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"` — 프로젝트 루트 경로 계산: 이 파일(py/...) 기준으로 상위 폴더를 BASE_DIR로 설정합니다.
- L023: `    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"` — 프로젝트 루트 경로 계산: 이 파일(py/...) 기준으로 상위 폴더를 BASE_DIR로 설정합니다.
- L024: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L025: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L026: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L027: `# Logging` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L028: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L029: `class _MaxLevelFilter(logging.Filter):` — 로그 필터 클래스 시작: 특정 레벨 이하 로그만 통과시키기 위한 Filter입니다.
- L030: `    def __init__(self, max_level: int):` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L031: `        super().__init__()` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L032: `        self._max_level = max_level` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L033: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L034: `    def filter(self, record: logging.LogRecord) -> bool:` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L035: `        return record.levelno <= self._max_level` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L036: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L037: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L038: `def setup_logging(base_dir: Path) -> Tuple[Path, Path]:` — 로깅 초기화 함수 시작: 콘솔 + 파일(.log/.log.err) 핸들러를 구성합니다.
- L039: `    run_date = datetime.now().strftime("%Y%m%d")` — 로그 날짜 폴더명 생성: YYYYMMDD (예: 20251228).
- L040: `    log_dir = base_dir / "log" / run_date` — 로그 디렉토리 경로 구성: BASE_DIR/log/YYYYMMDD.
- L041: `    log_dir.mkdir(parents=True, exist_ok=True)` — 로그 디렉토리 생성: parents=True로 상위 폴더까지 생성, exist_ok=True로 재실행 안전.
- L042: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L043: `    stamp = datetime.now().strftime("%H%M%S")` — 시각 스탬프 생성: HHMMSS (동일 날짜 내 여러 실행 구분).
- L044: `    base = "run_bq_var_json.{}.{}".format(stamp, os.getpid())` — PID 포함: 동시 실행 시 로그 파일명 충돌 방지.
- L045: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L046: `    out_log = log_dir / (base + ".log")` — 정상 로그 파일 경로: INFO/WARNING까지 기록될 .log 파일.
- L047: `    err_log = log_dir / (base + ".log.err")` — 에러 로그 파일 경로: ERROR 이상만 기록될 .log.err 파일.
- L048: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L049: `    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")` — 로그 포맷 지정: 시간/레벨/메시지 형태로 출력.
- L050: `    root = logging.getLogger()` — 루트 로거 획득: 모든 로거의 상위 로거에 핸들러를 붙입니다.
- L051: `    root.setLevel(logging.INFO)` — 루트 로거 레벨 설정: INFO 이상을 기본으로 기록.
- L052: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L053: `    for h in list(root.handlers):` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L054: `        root.removeHandler(h)` — 기존 핸들러 제거: 중복 로그 출력 방지(재호출 시 안전).
- L055: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L056: `    console = logging.StreamHandler()` — 콘솔 핸들러 생성: 터미널로 로그를 출력.
- L057: `    console.setLevel(logging.INFO)` — 루트 로거 레벨 설정: INFO 이상을 기본으로 기록.
- L058: `    console.setFormatter(fmt)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L059: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L060: `    file_out = logging.FileHandler(out_log, encoding="utf-8")` — 일반 로그 파일 핸들러 생성: out_log에 기록(utf-8).
- L061: `    file_out.setLevel(logging.INFO)` — 루트 로거 레벨 설정: INFO 이상을 기본으로 기록.
- L062: `    file_out.setFormatter(fmt)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L063: `    file_out.addFilter(_MaxLevelFilter(logging.WARNING))` — MaxLevelFilter 적용: WARNING 이하만 .log에 남기고 ERROR는 .log.err로 분리.
- L064: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L065: `    file_err = logging.FileHandler(err_log, encoding="utf-8")` — 에러 로그 파일 핸들러 생성: err_log에 ERROR 이상 기록.
- L066: `    file_err.setLevel(logging.ERROR)` — 루트 로거 레벨 설정: INFO 이상을 기본으로 기록.
- L067: `    file_err.setFormatter(fmt)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L068: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L069: `    root.addHandler(console)` — 루트 로거에 핸들러 등록: 콘솔/파일로 동시에 로그를 보냅니다.
- L070: `    root.addHandler(file_out)` — 루트 로거에 핸들러 등록: 콘솔/파일로 동시에 로그를 보냅니다.
- L071: `    root.addHandler(file_err)` — 루트 로거에 핸들러 등록: 콘솔/파일로 동시에 로그를 보냅니다.
- L072: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L073: `    return out_log, err_log` — 로그 파일 경로 반환: main()에서 사용자에게 경로를 안내하기 위함.
- L074: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L075: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L076: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L077: `# CSV/JSON handling` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L078: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L079: `def read_csv_records(csv_path: Path) -> List[Dict[str, str]]:` — CSV 읽기 함수 시작: bq.csv를 읽어 dict 레코드 리스트로 변환합니다.
- L080: `    """Read CSV and return list of record dicts."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L081: `    if not csv_path.exists():` — 입력 CSV 존재 확인: 없으면 즉시 예외로 중단해 원인 파악을 쉽게 합니다.
- L082: `        raise FileNotFoundError("CSV file not found: {}".format(csv_path))` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L083: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L084: `    text = csv_path.read_text(encoding="utf-8", errors="replace")` — CSV 전체 읽기: utf-8로 읽고, 깨진 문자는 대체(replace).
- L085: `    text = text.lstrip("\ufeff")  # Strip BOM` — BOM 제거: Excel 저장 UTF-8에서 흔한 BOM 때문에 헤더가 깨지는 문제 방지.
- L086: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L087: `    reader = csv.DictReader(text.splitlines())` — DictReader 사용: 첫 줄을 헤더로 보고 각 행을 dict로 변환합니다.
- L088: `    records = []` — 결과 리스트 초기화: 파싱된 레코드를 여기에 누적합니다.
- L089: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L090: `    for row in reader:` — 행 반복: CSV의 각 데이터 행(dict)을 순회합니다.
- L091: `        # Skip empty/comment rows` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L092: `        if not row or not any(row.values()):` — 빈 행 스킵: 완전히 비었거나 값이 모두 비어 있으면 건너뜁니다.
- L093: `            continue` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L094: `        first_val = next(iter(row.values()), "")` — 첫 컬럼 값 확인: 주석 행(#...)인지 판단하려고 첫 값만 꺼냅니다.
- L095: `        if first_val.strip().startswith("#"):` — 주석 행 스킵: 첫 값이 #으로 시작하면 해당 라인은 무시합니다.
- L096: `            continue` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L097: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L098: `        # Strip all values` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L099: `        records.append({k: v.strip() for k, v in row.items() if k})` — 레코드 적재: 모든 컬럼 값을 strip()한 뒤 records에 추가합니다.
- L100: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L101: `    return records` — 파싱 결과 반환: 이후 필터링/JSON 저장에 사용됩니다.
- L102: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L103: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L104: `def save_json(json_path: Path, records: List[Dict[str, str]]) -> None:` — JSON 저장 함수 시작: 레코드 리스트를 bq.json 기준정보로 저장합니다.
- L105: `    """Write records to JSON file."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L106: `    json_path.parent.mkdir(parents=True, exist_ok=True)` — JSON 저장 폴더 생성: src/list가 없으면 생성.
- L107: `    json_path.write_text(` — 파일 저장: JSON 문자열 + 개행을 utf-8로 기록.
- L108: `        json.dumps(records, ensure_ascii=False, indent=2) + "\n",` — JSON 문자열 생성: ensure_ascii=False(한글 유지), indent=2(가독성).
- L109: `        encoding="utf-8"` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L110: `    )` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L111: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L112: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L113: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L114: `# CLI parsing` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L115: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L116: `def parse_cli_args(argv: List[str]) -> Dict[str, str]:` — CLI 파싱 함수 시작: key=value 형태 인자를 dict로 변환합니다.
- L117: `    """Parse key=value arguments."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L118: `    args = {}` — 결과 dict 초기화: key=value 파싱 결과를 저장.
- L119: `    for token in argv:` — 인자 반복: 사용자가 입력한 각 토큰(mid=..., vs_job_dt=...)을 처리.
- L120: `        if "=" not in token:` — 형식 검증: key=value가 아니면 ValueError로 사용법 안내.
- L121: `            raise ValueError("Invalid arg (expected key=value): {}".format(token))` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L122: `        key, value = token.split("=", 1)` — key/value 분리: 첫 번째 = 기준으로만 분리(값에 =가 있어도 보호).
- L123: `        args[key.strip()] = value.strip()` — 공백 제거 후 저장: key와 value 양쪽 공백을 제거해 dict에 저장.
- L124: `    return args` — 파싱 결과 반환: 이후 필터/오버라이드로 사용됩니다.
- L125: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L126: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L127: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L128: `# BigQuery execution` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L129: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L130: `def quote_bq_string(value: str) -> str:` — BigQuery 문자열 이스케이프 함수 시작: 작은따옴표를 안전하게 처리합니다.
- L131: `    """Escape single quotes for BigQuery."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L132: `    return "'" + value.replace("'", "''") + "'"` — 작은따옴표 이스케이프: O'Brien → O''Brien 형태로 안전하게 만듭니다.
- L133: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L134: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L135: `def substitute_sql(template: str, pgm_id: str, job_dt: str, tbl_id: str) -> str:` — SQL 템플릿 치환 함수 시작: {vs_*} 플레이스홀더를 값으로 치환합니다.
- L136: `    """Replace placeholders with quoted values."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L137: `    return (` — 체이닝 replace: 3개 치환을 연속으로 수행해 최종 SQL 문자열을 만듭니다.
- L138: `        template.replace("{vs_pgm_id}", quote_bq_string(pgm_id))` — 플레이스홀더 치환: {vs_*} 토큰을 quote_bq_string() 결과로 바꿉니다.
- L139: `        .replace("{vs_job_dt}", quote_bq_string(job_dt))` — 체이닝 replace: 3개 치환을 연속으로 수행해 최종 SQL 문자열을 만듭니다.
- L140: `        .replace("{vs_tbl_id}", quote_bq_string(tbl_id))` — 체이닝 replace: 3개 치환을 연속으로 수행해 최종 SQL 문자열을 만듭니다.
- L141: `    )` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L142: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L143: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L144: `def run_bq_query(sql_text: str) -> None:` — bq query 실행 함수 시작: 표준 SQL 모드로 BigQuery CLI를 호출합니다.
- L145: `    """Execute BigQuery query."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L146: `    subprocess.run(` — 외부 명령 실행: bq query를 호출합니다(check=True로 실패 시 예외).
- L147: `        ["bq", "query", "--quiet", "--use_legacy_sql=false"],` — 표준 SQL 사용: 레거시 SQL을 비활성화합니다.
- L148: `        input=sql_text,` — STDIN 전달: SQL 텍스트를 표준입력으로 bq에 전달(파일 없이 실행).
- L149: `        universal_newlines=True,` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L150: `        check=True,` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L151: `    )` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L152: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L153: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L154: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L155: `# Core logic` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L156: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L157: `def apply_filters(records: List[Dict[str, str]], cli_args: Dict[str, str]) -> List[Dict[str, str]]:` — 필터 함수 시작: use_yn=Y + (옵션) mid / vs_pgm_id 필터를 적용합니다.
- L158: `    """Filter records by use_yn=Y and CLI filters (mid, vs_pgm_id)."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L159: `    # Filter: use_yn=Y (or missing)` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L160: `    targets = [r for r in records if r.get("use_yn", "Y").strip().upper() == "Y"]` — 기본 필터: use_yn이 Y(또는 누락)이면 실행 대상에 포함.
- L161: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L162: `    # CLI filter: mid` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L163: `    if "mid" in cli_args:` — 옵션 필터(mid): CLI에 mid가 있으면 해당 mid만 남깁니다.
- L164: `        targets = [r for r in targets if r.get("mid") == cli_args["mid"]]` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L165: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L166: `    # CLI filter: vs_pgm_id` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L167: `    if "vs_pgm_id" in cli_args:` — 옵션 필터(vs_pgm_id): 특정 SQL 파일만 실행할 때 사용.
- L168: `        targets = [r for r in targets if r.get("vs_pgm_id") == cli_args["vs_pgm_id"]]` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L169: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L170: `    return targets` — 필터 결과 반환: 이후 execute_sql_jobs()가 이 목록을 순회합니다.
- L171: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L172: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L173: `def execute_sql_jobs(targets: List[Dict[str, str]], overrides: Dict[str, str]) -> Tuple[int, int, int]:` — SQL 실행 함수 시작: 대상 레코드를 순회하며 SQL을 치환하고 bq로 실행합니다.
- L174: `    """Execute SQL for each target record. Returns (total, success, fail)."""` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L175: `    total = success = fail = 0` — 통계 카운터 초기화: 전체/성공/실패 건수를 집계합니다.
- L176: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L177: `    for record in targets:` — 대상 레코드 반복: 각 레코드(1행)마다 1개의 SQL 실행을 시도합니다.
- L178: `        # Apply overrides` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L179: `        effective = dict(record)` — 레코드 복사: 원본을 보존하기 위해 얕은 복사 dict를 만듭니다.
- L180: `        effective.update(overrides)` — 오버라이드 적용: CLI로 준 값(vs_job_dt 등)이 있으면 레코드 값을 덮어씁니다.
- L181: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L182: `        vs_pgm_id = effective.get("vs_pgm_id", "").strip()` — 필수 필드 추출: 실행할 SQL 파일명(vs_pgm_id)을 가져옵니다.
- L183: `        vs_job_dt = effective.get("vs_job_dt", "").strip()` — 파라미터 추출: SQL 템플릿에 넣을 작업일자(vs_job_dt).
- L184: `        vs_tbl_id = effective.get("vs_tbl_id", "").strip()` — 파라미터 추출: SQL 템플릿에 넣을 테이블ID(vs_tbl_id).
- L185: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L186: `        if not vs_pgm_id:` — 필수값 검증: vs_pgm_id가 없으면 실행 불가이므로 실패 처리.
- L187: `            logger.error("Missing vs_pgm_id in record: %s", effective)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L188: `            fail += 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L189: `            continue` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L190: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L191: `        # Resolve SQL file path` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L192: `        sql_path = Config.SQL_DIR / vs_pgm_id` — SQL 파일 경로 조립: sql/ + 파일명으로 실제 파일 경로를 만듭니다.
- L193: `        if not sql_path.exists():` — 파일 존재 검증: SQL 파일이 없으면 실패 처리.
- L194: `            logger.error("SQL file not found: %s", sql_path)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L195: `            fail += 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L196: `            continue` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L197: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L198: `        # Execute` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L199: `        total += 1` — 실행 시도 카운트 증가: 유효한 vs_pgm_id/파일 존재 시점에 total 증가.
- L200: `        logger.info(` — 실행 로그 출력: 어떤 파일을 어떤 파라미터로 실행하는지 기록.
- L201: `            "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s)",` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L202: `            vs_pgm_id,` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L203: `            effective.get("mid", ""),` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L204: `            vs_job_dt,` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L205: `            vs_tbl_id,` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L206: `        )` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L207: `        ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L208: `        try:` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L209: `            pgm_id = sql_path.stem` — 프로그램 ID 계산: 파일명에서 확장자 제거(.sql 제외)한 값을 사용.
- L210: `            template = sql_path.read_text(encoding="utf-8", errors="replace")` — SQL 템플릿 읽기: 파일을 읽어 문자열로 가져옵니다.
- L211: `            sql_text = substitute_sql(template, pgm_id, vs_job_dt, vs_tbl_id)` — 템플릿 치환: {vs_*} 토큰을 실제 값으로 바꾼 최종 SQL 생성.
- L212: `            run_bq_query(sql_text)` — BigQuery 실행: bq query로 SQL을 실행합니다.
- L213: `            success += 1` — 성공 카운트 증가: bq query가 정상 종료했을 때.
- L214: `        except subprocess.CalledProcessError as e:` — 실행 실패 처리: bq 명령 실패(비정상 종료 코드) 예외를 잡아 실패로 집계.
- L215: `            logger.error("bq query failed (exit_code=%s)", e.returncode)` — 종료 코드 기록: 실패 원인 파악을 위해 returncode를 로그로 남깁니다.
- L216: `            fail += 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L217: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L218: `    return total, success, fail` — 통계 반환: main()에서 요약 로그 및 종료 코드 계산에 사용.
- L219: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L220: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L221: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L222: `# Entry Point` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L223: `# ============================` — 섹션 구분 주석: 파일 구조를 눈으로 빠르게 구분하기 위한 표시입니다.
- L224: `def main() -> int:` — 메인 함수 시작: 전체 실행 흐름(로깅→파싱→CSV→필터→실행→요약)을 조율합니다.
- L225: `    out_log, err_log = setup_logging(Config.BASE_DIR)` — 로깅 설정 호출: 콘솔/파일 로그 준비 후 로그 파일 경로를 받습니다.
- L226: `    logger.info("SUCCESS LOG : %s", out_log)` — 정상 로그 파일 위치 안내: 실행 후 .log 파일을 쉽게 찾도록 출력.
- L227: `    logger.info("ERROR LOG   : %s", err_log)` — 에러 로그 파일 위치 안내: 오류는 .log.err에서 우선 확인.
- L228: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L229: `    # Parse CLI arguments` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L230: `    try:` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L231: `        cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}` — CLI 인자 파싱: key=value 형태 인자를 dict로 변환합니다.
- L232: `    except ValueError as e:` — 입력 형식 오류 처리: 잘못된 인자면 사용법을 출력하고 종료.
- L233: `        logger.error("%s", e)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L234: `        logger.error(` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L235: `            "Usage: python %s [mid=<mid>] [vs_pgm_id=<file.sql>] [vs_job_dt=<yyyymmdd>]",` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L236: `            sys.argv[0]` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L237: `        )` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L238: `        return 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L239: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L240: `    # Read CSV and generate JSON baseline` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L241: `    try:` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L242: `        records = read_csv_records(Config.CSV_PATH)` — CSV 읽기: src/list/bq.csv를 레코드 목록으로 파싱합니다.
- L243: `        save_json(Config.JSON_PATH, records)` — JSON 생성: 파싱된 레코드를 src/list/bq.json으로 저장합니다.
- L244: `        logger.info("Generated JSON baseline: %s", Config.JSON_PATH)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L245: `    except Exception as e:` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L246: `        logger.error("%s", e)` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L247: `        return 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L248: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L249: `    # Filter records` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L250: `    targets = apply_filters(records, cli_args)` — 대상 필터링: use_yn=Y 및 (선택) mid/vs_pgm_id 조건을 적용.
- L251: `    if not targets:` — 대상 없음 처리: 실행할 행이 없으면 에러로 종료(사용자에게 원인 알림).
- L252: `        logger.error("No target rows matched the given filters (and use_yn=Y).")` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L253: `        return 1` — 해당 라인의 동작은 주변 문맥(현재 함수/블록)에 의해 결정됩니다.
- L254: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L255: `    # Extract overrides (non-filter CLI args)` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L256: `    overrides = {k: v for k, v in cli_args.items() if k not in ("mid", "vs_pgm_id")}` — 오버라이드 추출: mid/vs_pgm_id는 필터용이므로 제외하고 나머지를 덮어쓰기용으로 사용.
- L257: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L258: `    # Execute SQL jobs` — 주석: 코드의 의도/구조를 설명하거나 구분선을 제공합니다.
- L259: `    total, success, fail = execute_sql_jobs(targets, overrides)` — 실행 위임: 실제 SQL 실행/통계 집계는 execute_sql_jobs()가 담당.
- L260: `    ` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L261: `    logger.info("SUMMARY total=%s, success=%s, fail=%s", total, success, fail)` — 요약 로그 출력: total/success/fail 통계를 기록합니다.
- L262: `    return 0 if fail == 0 else 1` — 종료 코드 결정: 실패가 0이면 0(성공), 하나라도 있으면 1(실패).
- L263: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L264: `` — 빈 줄: 가독성을 위해 섹션/논리 블록을 분리합니다.
- L265: `if __name__ == "__main__":` — 스크립트 진입점: 이 파일을 직접 실행할 때만 main()을 호출합니다.
- L266: `    raise SystemExit(main())` — 프로세스 종료: main() 반환값을 운영체제 종료 코드로 전달합니다.

---

## 보충 설명: execute_sql_jobs 분리 이유

`execute_sql_jobs()`는 main()의 “흐름 조율”과, “실제 실행(반복/예외처리/집계)”를 분리합니다.

- main(): 입력/필터/오버라이드 준비 + 실행 호출 + 요약

- execute_sql_jobs(): 각 레코드에 대해 SQL 파일 읽기 → 치환 → bq 실행 → 성공/실패 집계


---

## 마지막 업데이트

- 날짜: 2025-12-28

- 기준 소스: py/run_bq_var_json.py

