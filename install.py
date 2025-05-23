import shutil
import os

packages_to_remove = [
    'streamlit', 'pandas', 'numpy', 'openpyxl', 'msoffcrypto_tool',
    'supabase', 'dotenv', 'python_dotenv', 'xlrd', 'xlwt'
]

site_packages = r'C:\Users\SPM\AppData\Local\Programs\Python\Python313\Lib\site-packages'

for pkg in packages_to_remove:
    for item in os.listdir(site_packages):
        if item.lower().startswith(pkg.lower()):
            path = os.path.join(site_packages, item)
            print(f"Removing: {path}")
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception as e:
                print(f"Failed to remove {path}: {e}")