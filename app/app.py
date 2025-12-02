import os
import pandas as pd
print("ShopZada app has started!")

base_path = "data"

def list_department_files():
    depts = os.listdir(base_path)
    return depts

def load_dataset(department_folder):
    path = os.path.join(base_path, department_folder)
    files = os.listdir(path)
    print(f"Files in {department_folder}:", files)
    return files

if __name__ == "__main__":
    print("Departments:", list_department_files())
