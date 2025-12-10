class View:
    @staticmethod
    def show_menu():
        print("\n=== MAIN MENU ===")
        print("1. Show students         4. Add student          7. Edit student             10. Delete student")
        print("2. Show supervisors      5. Add supervisor       8. Edit supervisor          11. Delete supervisor")
        print("3. Show projects         6. Add project          9. Edit project             12. Delete project")
        print("\n13. Generate random data")
        print("14. Search: grade range + student name")
        print("15. Search: date range + supervisor department")
        print("16. Search: student stats by project status")
        print("0. Exit")

    @staticmethod
    def show_data(data):
        if not data:
            print("No data.")
            return
        for row in data:
            print(row)
