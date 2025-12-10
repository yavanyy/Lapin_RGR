from model import Database
from view import View
from datetime import datetime

class Controller:
    def __init__(self):
        self.db = Database()
        self.view = View()

    def input_int(self, prompt, min_value=None, max_value=None):
        while True:
            try:
                val = int(input(prompt))
                if min_value is not None and val < min_value:
                    print(f"Value must be >= {min_value}")
                    continue
                if max_value is not None and val > max_value:
                    print(f"Value must be <= {max_value}")
                    continue
                return val
            except ValueError:
                print("Invalid number format. Try again.")

    def input_date(self, prompt, format_="%Y-%m-%d"):
        while True:
            s = input(prompt)
            try:
                dt = datetime.strptime(s, format_).date()
                return dt
            except ValueError:
                print(f"Invalid date format. Use {format_} (e.g. 2025-11-16).")

    def input_str_nonempty(self, prompt):
        while True:
            s = input(prompt).strip()
            if s == "":
                print("Field cannot be empty.")
                continue
            return s

    def run(self):
        while True:
            self.view.show_menu()
            choice = input("\nYour choice: ")

            if choice == "0":
                print("Exit...")
                self.db.close()
                break

            elif choice == "1":
                rows = self.db.get_students()
                self.view.show_data(rows)

            elif choice == "2":
                rows = self.db.get_supervisors()
                self.view.show_data(rows)

            elif choice == "3":
                rows = self.db.get_projects()
                self.view.show_data(rows)

            elif choice == "4":
                name = self.input_str_nonempty("Student name: ")
                email = self.input_str_nonempty("Student email: ")
                group = self.input_str_nonempty("Group (e.g. КВ-31): ")
                self.db.add_student(name, email, group)

            elif choice == "5":
                name = self.input_str_nonempty("Supervisor name: ")
                department = self.input_str_nonempty("Department: ")
                email = self.input_str_nonempty("Email: ")
                self.db.add_supervisor(name, department, email)

            elif choice == "6":
                title = self.input_str_nonempty("Project title: ")
                date = self.input_date("Start date (YYYY-MM-DD): ")
                status = self.input_str_nonempty("Status (active/completed): ").strip().lower()

                if status == "completed":
                    grade = self.input_int("Grade (0-100): ", min_value=0, max_value=100)
                else:
                    print("Project is not completed yet, grade will be set to NULL.")
                    grade = None

                supervisor_id = self.input_int("Supervisor ID: ", min_value=1)
                student_id = self.input_int("Student ID: ", min_value=1)
                self.db.add_project(title, date, status, grade, supervisor_id, student_id)

            elif choice == "7":
                sid = self.input_int("Student ID: ", min_value=1)
                name = self.input_str_nonempty("New name: ")
                email = self.input_str_nonempty("New email: ")
                group = self.input_str_nonempty("New group: ")
                self.db.edit_student(sid, name, email, group)

            elif choice == "8":
                sup_id = self.input_int("Supervisor ID: ", min_value=1)
                name = self.input_str_nonempty("New name: ")
                department = self.input_str_nonempty("New department: ")
                email = self.input_str_nonempty("New email: ")
                self.db.edit_supervisor(sup_id, name, department, email)

            elif choice == "9":
                pid = self.input_int("Project ID: ", min_value=1)
                title = self.input_str_nonempty("New title: ")
                date = self.input_date("New start date (YYYY-MM-DD): ")
                status = self.input_str_nonempty("New status (active/completed): ").strip().lower()

                if status == "completed":
                    grade = self.input_int("New grade (0-100): ", min_value=0, max_value=100)
                else:
                    print("Project is not completed, grade will be set to NULL.")
                    grade = None

                supervisor_id = self.input_int("Supervisor ID: ", min_value=1)
                student_id = self.input_int("Student ID: ", min_value=1)
                self.db.edit_project(pid, title, date, status, grade, supervisor_id, student_id)

            elif choice == "10":
                sid = self.input_int("Student ID: ", min_value=1)
                self.db.delete_student(sid)

            elif choice == "11":
                sup_id = self.input_int("Supervisor ID: ", min_value=1)
                self.db.delete_supervisor(sup_id)

            elif choice == "12":
                pid = self.input_int("Project ID: ", min_value=1)
                self.db.delete_project(pid)

            elif choice == "13":
                count = self.input_int("How many rows to generate?: ", min_value=1)
                self.db.generate_random_data(count)

            elif choice == "14":
                min_g = self.input_int("Min grade: ", min_value=0, max_value=100)
                max_g = self.input_int("Max grade: ", min_value=0, max_value=100)
                name_pat = input("Student name pattern (use % for LIKE): ")
                rows, dur = self.db.search_1_projects_by_grade_and_student_name(
                    min_g, max_g, name_pat
                )
                print(
                    f"\n=== RESULTS FOR grade {min_g}..{max_g} "
                    f"AND student name ILIKE '{name_pat}' ==="
                )
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            elif choice == "15":
                start = self.input_date("Start date (YYYY-MM-DD): ")
                end = self.input_date("End date (YYYY-MM-DD): ")
                dept_pat = input("Department pattern (use % for LIKE): ")
                rows, dur = self.db.search_2_projects_by_date_and_supervisor_department(
                    start, end, dept_pat
                )
                print(
                    f"\n=== RESULTS FOR date {start}..{end} "
                    f"AND department ILIKE '{dept_pat}' ==="
                )
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            elif choice == "16":
                status_pat = input("Project status pattern (use % for LIKE): ")
                min_avg = self.input_int("Min average grade: ", min_value=0, max_value=100)
                rows, dur = self.db.search_3_students_stats_by_project_status(
                    status_pat, min_avg
                )
                print(
                    f"\n=== RESULTS FOR status ILIKE '{status_pat}' "
                    f"AND avg_grade >= {min_avg} ==="
                )
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            else:
                print("Wrong choice! Try again.")
