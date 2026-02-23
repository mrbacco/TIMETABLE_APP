<!--
"""
TEACHERS APP project 
Timetable allocation tool done with flask and AI powered

mrbacco04@gmail.com
Feb 23, 2026 

"""
-->
# Teacher Time Allocator (Python + SQLite)

Flask web app with full CRUD for:
- Teachers
- Skills

And a weekly timetable session allocator using:
- Day tabs (Monday to Friday)
- Period rows (8:00 to 15:00 with lunch 12:00 to 13:00)
- Grade/Class columns (Grade 1 to Grade 5)
- Skill matching + teacher free-time validation
- Teacher conflict prevention in same period (busy teachers are disabled)

## Database
- Engine: SQLite
- File: `teachers_app.db`
- ORM: Flask-SQLAlchemy
- Core tables: `teachers`, `skills`, `sessions`
- Join table: `teacher_skills` (many-to-many between teachers and skills)

## Run
1. Create and activate a virtual environment (optional):
   - `python -m venv .venv`
   - `.\.venv\Scripts\activate`
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Start the app:
   - `python app.py`
4. Open:
   - `http://127.0.0.1:8000`

## Teacher Availability Format
Manual teacher creation defaults to full weekly availability (all teaching periods, Monday-Friday).

To set specific availability, use CSV import with `free_slots`, for example:
- `Mon 08:00-09:00, Tue 09:00, Friday 13:00-14:00`

The app accepts both day short names (`Mon`) and full names (`Monday`), and both start-time only (`09:00`) or full range (`09:00-10:00`).

## Bulk Import CSV
Excel is supported by exporting sheets as CSV.

### Skills CSV
Headers:
- `name` (also supports `skill`, `skill_name`)

Example:
```csv
name
Math
Science
English
```

### Teachers CSV
Headers:
- `name`
- `skills` (for example `Math|Science`)
- `free_slots` optional (for example `Mon 09:00|Tue 10:00`)

Delimiters supported inside `free_slots` and `skills`: `|`, `;`, or `,`

Example:
```csv
name,free_slots,skills
"Ms. Lee","Mon 09:00|Tue 10:00","Math|Science"
"Mr. Cruz","Wed 08:00|Thu 11:00","English"
```

Example without `free_slots`:
```csv
name,skills
"Ms. Lee","Math|Science"
"Mr. Cruz","English"
```

If a teacher row references a missing skill, that skill is auto-created during import.
If `free_slots` is missing/blank in a row, full weekly availability is applied by default.

## Optional
- Set host/port:
  - PowerShell: `$env:HOST="127.0.0.1"; $env:PORT="8080"; python app.py`
- Set custom database URL:
  - PowerShell: `$env:DATABASE_URL="sqlite:///teachers_app.db"`
