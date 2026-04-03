# Prompt Skeleton Service

Config-driven FastAPI service for assembling prompt skeleton packages for Dify.

## Directory

```text
prompt_skeleton_service/
├─ app/
│  ├─ core/
│  ├─ routers/
│  ├─ schemas/
│  ├─ services/
│  └─ main.py
├─ configs/
│  └─ types/
│     ├─ continuation.yaml
│     ├─ detail.yaml
│     ├─ intent.yaml
│     ├─ main_idea.yaml
│     └─ sentence_fill.yaml
├─ pyproject.toml
└─ README.md
```

## Features

- Dynamic `question_type` loading from `configs/types/*.yaml`
- Dynamic `slot_schema` validation at runtime
- Multi-pattern support per type
- Pattern auto-selection via `match_rules`
- Difficulty projection into unified skeleton metrics
- Prompt package assembly without calling any real LLM
- Hot reload for configs through API

## Run

```bash
cd prompt_skeleton_service
python -m venv .venv
.venv\Scripts\activate
pip install -e .
uvicorn app.main:app --reload
```

## API

- `GET /api/v1/types`
- `GET /api/v1/types/{question_type}/schema`
- `POST /api/v1/slots/resolve`
- `POST /api/v1/prompt/build`
- `POST /api/v1/admin/reload-config`

## Curl

```bash
curl http://127.0.0.1:8000/api/v1/types
```

```bash
curl http://127.0.0.1:8000/api/v1/types/continuation/schema
```

```bash
curl -X POST http://127.0.0.1:8000/api/v1/slots/resolve ^
  -H "Content-Type: application/json" ^
  -d "{\"question_type\":\"continuation\",\"difficulty_target\":\"medium\",\"type_slots\":{\"anchor_focus\":\"new_problem\",\"reasoning_focus\":\"forward_inference\"}}"
```

```bash
curl -X POST http://127.0.0.1:8000/api/v1/prompt/build ^
  -H "Content-Type: application/json" ^
  -d "{\"question_type\":\"sentence_fill\",\"difficulty_target\":\"hard\",\"topic\":\"教育公平\",\"count\":2,\"passage_style\":\"说明文\",\"type_slots\":{\"blank_role\":\"summary\",\"clue_density\":\"low\"}}"
```

```bash
curl -X POST http://127.0.0.1:8000/api/v1/admin/reload-config
```
