import psycopg2
import time
import logging
import traceback
from psycopg2 import errors

logging.basicConfig(
    filename='db_errors.log',
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s %(message)s'
)

class Database:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                dbname='student_projects',
                user='postgres',
                password='9908',
                host='localhost',
                port=5432
        )
            self.cur = self.conn.cursor()
            print("Connected to PostgreSQL successfully!")
        except Exception:
            logging.error("Error connecting to PostgreSQL:\n" + traceback.format_exc())
            print("Error: Failed to connect to database (details in db_errors.log).")
            self.conn = None
            self.cur = None

    def execute(self, query, params=None, fetch=False, report_time=False):
        if self.conn is None or self.cur is None:
            print("Error: Database connection is not established.")
            default = [] if fetch else None
            return (default, 0.0) if report_time else default

        start_time = time.monotonic() if report_time else None

        try:
            self.cur.execute(query, params or ())

            rows = self.cur.fetchall() if fetch else None

            first_word = query.lstrip().split()[0].upper()
            if first_word not in ("SELECT", "WITH"):
                self.conn.commit()

            duration_ms = (time.monotonic() - start_time) * 1000 if report_time else None

            if report_time:
                return (rows, duration_ms) if fetch else (None, duration_ms)

            return rows

        except errors.ForeignKeyViolation:
            logging.error("ForeignKeyViolation:\n" + traceback.format_exc())
            print("Error: foreign key violation (related record exists / is missing).")
            self.conn.rollback()
        except errors.UniqueViolation:
            logging.error("UniqueViolation:\n" + traceback.format_exc())
            print("Error: uniqueness violation (duplicate key).")
            self.conn.rollback()
        except Exception:
            logging.error("SQL execution error:\n" + traceback.format_exc())
            print("SQL execution error: operation not performed (details in db_errors.log).")
            self.conn.rollback()

        default = [] if fetch else None
        return (default, None) if report_time else default


    def add_student(self, name, email, group_name):
        query = """
            INSERT INTO public.student (name, email, "group")
            VALUES (%s, %s, %s)
            RETURNING student_id;
        """
        res = self.execute(query, (name, email, group_name), fetch=True)
        if res:
            new_id = res[0][0]
            print(f"Added student id={new_id}")
            return new_id
        return None

    def edit_student(self, student_id, name, email, group_name):
        query = """
            UPDATE public.student
            SET name=%s, email=%s, "group"=%s
            WHERE student_id=%s;
        """
        self.execute(query, (name, email, group_name, student_id))

    def delete_student(self, student_id):
        count = self.execute(
            "SELECT COUNT(*) FROM public.project WHERE student_id = %s;",
            (student_id,), fetch=True
        )
        if count and count[0][0] > 0:
            print(
                f"Error: Unable to delete student (ID: {student_id}). "
                f"He/She has {count[0][0]} related projects."
            )
            return
        self.execute("DELETE FROM public.student WHERE student_id = %s;", (student_id,))

    def get_students(self):
        return self.execute(
            'SELECT student_id, name, email, "group" FROM public.student ORDER BY student_id;',
            fetch=True
        )

    def add_supervisor(self, name, department, email):
        query = """
            INSERT INTO public.supervisor (name, department, email)
            VALUES (%s, %s, %s)
            RETURNING supervisor_id;
        """
        res = self.execute(query, (name, department, email), fetch=True)
        if res:
            new_id = res[0][0]
            print(f"Added supervisor id={new_id}")
            return new_id
        return None

    def edit_supervisor(self, supervisor_id, name, department, email):
        query = """
            UPDATE public.supervisor
            SET name=%s, department=%s, email=%s
            WHERE supervisor_id=%s;
        """
        self.execute(query, (name, department, email, supervisor_id))

    def delete_supervisor(self, supervisor_id):
        count = self.execute(
            "SELECT COUNT(*) FROM public.project WHERE supervisor_id = %s;",
            (supervisor_id,), fetch=True
        )
        if count and count[0][0] > 0:
            print(
                f"Error: Unable to delete supervisor (ID: {supervisor_id}). "
                f"He/She has {count[0][0]} related projects."
            )
            return
        self.execute("DELETE FROM public.supervisor WHERE supervisor_id = %s;", (supervisor_id,))

    def get_supervisors(self):
        return self.execute(
            "SELECT supervisor_id, name, department, email "
            "FROM public.supervisor ORDER BY supervisor_id;",
            fetch=True
        )

    def add_project(self, title, start_date, status, grade, supervisor_id, student_id):
        query = """
            INSERT INTO public.project (title, start_date, status, grade, supervisor_id, student_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING project_id;
        """
        res = self.execute(query, (title, start_date, status, grade, supervisor_id, student_id), fetch=True)
        if res:
            new_id = res[0][0]
            print(f"Added project id={new_id}")
            return new_id
        return None

    def edit_project(self, project_id, title, start_date, status, grade, supervisor_id, student_id):
        query = """
            UPDATE public.project
            SET title=%s, start_date=%s, status=%s, grade=%s,
                supervisor_id=%s, student_id=%s
            WHERE project_id=%s;
        """
        self.execute(query, (title, start_date, status, grade, supervisor_id, student_id, project_id))

    def delete_project(self, project_id):
        self.execute("DELETE FROM public.project WHERE project_id = %s;", (project_id,))

    def get_projects(self):
        return self.execute(
            """SELECT project_id, title, start_date, status, grade,
                      supervisor_id, student_id
               FROM public.project
               ORDER BY project_id;""",
            fetch=True
        )

    def generate_random_data(self, count):
        print("Clearing tables...")
        self.execute('TRUNCATE public.project, public.supervisor, public.student '
                     'RESTART IDENTITY CASCADE;')

        print(f"Generating {count} rows...")

        student_names = ['Ivan Lapin', 'Taras Shevchenko',
                         'Anastasiia Pshonna', 'Mykhailo Kobzar',
                         'Maryna Starovoit']
        student_groups = ['КВ-31', 'КВ-32', 'КВ-33', 'КВ-34', 'КВ-35']

        supervisor_names = [
            'Ivan Ivanov',
            'Petro Petrenko',
            'Oleg Sydorenko',
            'Anna Bondar',
            'Olena Kovalenko'
        ]

        departments = ['FIOT', 'FPM', 'FMM', 'FEAM', 'FSP']

        statuses = ['active', 'completed']

        student_query = """
            INSERT INTO public.student (name, email, "group")
            SELECT
                name,
                lower(replace(name, ' ', '.')) || '@lll.kpi.ua' AS email,
                (ARRAY[%s, %s, %s, %s, %s])[ceil(random()*5)::int]  -- group
            FROM (
                SELECT
                    (ARRAY[%s, %s, %s, %s, %s])[ceil(random()*5)::int] AS name
                FROM generate_series(1, %s) AS gs
            ) t;
        """
        self.execute(
            student_query,
            (*student_groups, *student_names, count)
        )

        supervisor_query = """
            INSERT INTO public.supervisor (name, department, email)
            SELECT
                name,
                department,
                lower(replace(name, ' ', '.')) || '@lll.kpi.ua' AS email
            FROM (
                SELECT
                    (ARRAY[%s, %s, %s, %s, %s])[ceil(random()*5)::int] AS name,
                    (ARRAY[%s, %s, %s, %s, %s])[ceil(random()*5)::int] AS department
                FROM generate_series(1, %s) AS gs
            ) t;
        """
        self.execute(
            supervisor_query,
            (*supervisor_names, *departments, count)
        )

        project_query = """
            INSERT INTO public.project (title, start_date, status, grade, supervisor_id, student_id)
            SELECT
                'Project #' || gs::text,
                start_date,
                status,
                CASE
                    WHEN status = 'completed'
                        THEN (60 + floor(random()*41))::int   -- 60..100
                    ELSE NULL
                END AS grade,
                supervisor_id,
                student_id
            FROM (
                SELECT
                    gs,
                    date '2025-01-01' + ((random()*180)::int) * interval '1 day' AS start_date,
                    (ARRAY['active', 'completed'])[ceil(random()*2)::int] AS status,
                    s.supervisor_id,
                    st.student_id
                FROM generate_series(1, %s) AS gs
                JOIN (
                    SELECT supervisor_id,
                           ROW_NUMBER() OVER (ORDER BY random()) AS rn
                    FROM public.supervisor
                ) s ON s.rn = gs
                JOIN (
                    SELECT student_id,
                           ROW_NUMBER() OVER (ORDER BY random()) AS rn
                    FROM public.student
                ) st ON st.rn = gs
            ) t;
        """
        self.execute(project_query, (count,))

        self.sync_sequences('public.student', 'student_id')
        self.sync_sequences('public.supervisor', 'supervisor_id')
        self.sync_sequences('public.project', 'project_id')

        print("Data generation completed successfully.")

    def sync_sequences(self, table_name, pk_column):
        try:
            seq_sql = "SELECT pg_get_serial_sequence(%s, %s);"
            seq_name = self.execute(seq_sql, (table_name, pk_column), fetch=True)
            if seq_name and seq_name[0][0]:
                seq = seq_name[0][0]
                setval_sql = (
                    f"SELECT setval(%s, "
                    f"COALESCE((SELECT MAX({pk_column}) FROM {table_name}), 0) + 1, false);"
                )
                self.execute(setval_sql, (seq,))
                print(f"Sequence for {table_name}.{pk_column} synced to MAX+1.")
            else:
                print(f"No serial sequence detected for {table_name}.{pk_column}.")
        except Exception:
            logging.error("Error syncing sequence:\n" + traceback.format_exc())
            print("Warning: failed to automatically synchronize sequence (details in db_errors.log).")

    def search_1_projects_by_grade_and_student_name(self, min_grade, max_grade, name_pattern):
        query = """
        SELECT p.project_id, p.title, p.grade, s.name AS student_name, s."group"
        FROM public.project p
        JOIN public.student s ON p.student_id = s.student_id
        WHERE p.grade BETWEEN %s AND %s
          AND s.name ILIKE %s
        ORDER BY p.grade DESC;
        """
        return self.execute(
            query,
            (min_grade, max_grade, name_pattern),
            fetch=True,
            report_time=True
        )

    def search_2_projects_by_date_and_supervisor_department(self, start_date, end_date, dept_pattern):
        query = """
        SELECT p.project_id, p.title, p.start_date, p.status,
               sup.name AS supervisor_name, sup.department
        FROM public.project p
        JOIN public.supervisor sup ON p.supervisor_id = sup.supervisor_id
        WHERE p.start_date BETWEEN %s AND %s
          AND sup.department ILIKE %s
        ORDER BY p.start_date;
        """
        return self.execute(
            query,
            (start_date, end_date, dept_pattern),
            fetch=True,
            report_time=True
        )

    def search_3_students_stats_by_project_status(self, status_pattern, min_avg_grade):
        query = """
        SELECT
            st.student_id,
            st.name AS student_name,
            st."group",
            COUNT(p.project_id) AS total_projects,
            AVG(p.grade) AS avg_grade
        FROM public.student st
        JOIN public.project p ON p.student_id = st.student_id
        WHERE p.status ILIKE %s
        GROUP BY st.student_id, st.name, st."group"
        HAVING AVG(p.grade) >= %s
        ORDER BY avg_grade DESC;
        """
        return self.execute(
            query,
            (status_pattern, min_avg_grade),
            fetch=True,
            report_time=True
        )


    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
