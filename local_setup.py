import os
import sys
import json
import shutil
import builtins
import pathlib
from pathlib import Path
from pyspark.sql import SparkSession

# Force local loopback IP for Spark
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Resolve project root folder (where local_setup.py lives)
PROJECT_ROOT = Path(__file__).absolute().parent
CATALOG_ROOT = PROJECT_ROOT / "catalog"

# Initialize local Spark Session with Delta Lake support
print("Initializing local Spark session with Delta Lake support...")
builder = SparkSession.builder \
    .appName("ClaimsProcessingLocal") \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", str(CATALOG_ROOT / "warehouse")) \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={CATALOG_ROOT}/metastore/metastore_db;create=true") \
    .config("spark.sql.shuffle.partitions", "4")

from delta import configure_spark_with_delta_pip
spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("Spark session successfully created.")

# Ensure local development schemas exist
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# ==============================================================================
# MONKEYPATCH FILE SYSTEM AND SPARK FOR LOCAL SANDBOX EXECUTION
# ==============================================================================
# Helper to translate Volume paths to local absolute path under catalog/bronze
def translate_local_volumes(path):
    if isinstance(path, (str, Path)):
        path_str = str(path)
        if path_str.startswith("file:"):
            scheme = "file:"
            inner_path = path_str[5:]
        else:
            scheme = ""
            inner_path = path_str

        if inner_path == "/Volumes":
            local_path_str = scheme + str((CATALOG_ROOT / "bronze").absolute())
            return Path(local_path_str) if isinstance(path, Path) else local_path_str
        elif inner_path.startswith("/Volumes/"):
            local_path_str = scheme + str((CATALOG_ROOT / "bronze" / inner_path[9:]).absolute())
            return Path(local_path_str) if isinstance(path, Path) else local_path_str
    return path

# 1. Patch pathlib._normal_accessor methods directly (since they map to built-in C functions)
def wrap_pathlib_accessor(method_name):
    if hasattr(pathlib._normal_accessor, method_name):
        orig_method = getattr(pathlib._normal_accessor, method_name)
        def patched_method(pathobj, *args, **kwargs):
            return orig_method(translate_local_volumes(pathobj), *args, **kwargs)
        setattr(pathlib._normal_accessor, method_name, patched_method)

for method in ['mkdir', 'stat', 'open', 'scandir', 'listdir', 'touch', 'unlink', 'rmdir', 'rename', 'replace']:
    wrap_pathlib_accessor(method)

# 2. Translate /Volumes/... paths in standard Python os / built-in operations
_orig_open = builtins.open
_orig_makedirs = os.makedirs
_orig_mkdir = os.mkdir
_orig_stat = os.stat
_orig_listdir = os.listdir

def patched_open(file, *args, **kwargs):
    return _orig_open(translate_local_volumes(file), *args, **kwargs)
    
def patched_makedirs(name, *args, **kwargs):
    return _orig_makedirs(translate_local_volumes(name), *args, **kwargs)

def patched_mkdir(path, *args, **kwargs):
    return _orig_mkdir(translate_local_volumes(path), *args, **kwargs)

def patched_stat(path, *args, **kwargs):
    return _orig_stat(translate_local_volumes(path), *args, **kwargs)

def patched_listdir(path, *args, **kwargs):
    return _orig_listdir(translate_local_volumes(path), *args, **kwargs)

builtins.open = patched_open
os.makedirs = patched_makedirs
os.mkdir = patched_mkdir
os.stat = patched_stat
os.listdir = patched_listdir

# 3. Patch Spark SQL to translate 3-part namespaces to 2-part namespaces
_orig_sql = SparkSession.sql
_orig_table = SparkSession.table

def patched_sql(self, sqlQuery, *args, **kwargs):
    clean_query = sqlQuery
    for cat in ["274", "new"]:
        clean_query = clean_query.replace(f"`{cat}`.", "")
        clean_query = clean_query.replace(f"{cat}.", "")
        
    # Prevent DeltaCatalog single-part namespace bug on local DDL duplicate runs
    import re
    match = re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z0-9_`\.]+)", clean_query, re.IGNORECASE)
    if match:
        table_name = match.group(1).replace("`", "")
        parts = table_name.split(".")
        if len(parts) == 2:
            db_name, tbl_name = parts[0], parts[1]
        else:
            db_name, tbl_name = "default", parts[0]
            
        # Check if Delta table directory exists and is populated in the warehouse
        table_dir = CATALOG_ROOT / "warehouse" / f"{db_name}.db" / tbl_name
        alt_table_dir = CATALOG_ROOT / "warehouse" / tbl_name
        
        table_exists = False
        try:
            if (table_dir.exists() and any(table_dir.iterdir())) or (alt_table_dir.exists() and any(alt_table_dir.iterdir())):
                table_exists = True
        except Exception:
            pass
            
        if table_exists:
            print(f"[LOCAL SETUP] Table {table_name} already exists. Skipping DDL execution.")
            return self.createDataFrame([], schema="id INT")
            
    return _orig_sql(self, clean_query, *args, **kwargs)

def patched_table(self, tableName, *args, **kwargs):
    clean_name = tableName
    for cat in ["274", "new"]:
        clean_name = clean_name.replace(f"`{cat}`.", "")
        clean_name = clean_name.replace(f"{cat}.", "")
    return _orig_table(self, clean_name, *args, **kwargs)

SparkSession.sql = patched_sql
SparkSession.table = patched_table

# 4. Patch Spark DataFrame Reader & Writer to translate /Volumes paths passed to Java executors
from pyspark.sql import DataFrameReader, DataFrameWriter

_orig_load = DataFrameReader.load
_orig_csv = DataFrameReader.csv
_orig_parquet = DataFrameReader.parquet
_orig_json = DataFrameReader.json

def patched_load(self, path=None, *args, **kwargs):
    if path is not None:
        path = translate_local_volumes(path)
    if 'paths' in kwargs:
        kwargs['paths'] = [translate_local_volumes(p) for p in kwargs['paths']]
    return _orig_load(self, path, *args, **kwargs)

def patched_csv(self, path, *args, **kwargs):
    return _orig_csv(self, translate_local_volumes(path), *args, **kwargs)

def patched_parquet(self, path, *args, **kwargs):
    return _orig_parquet(self, translate_local_volumes(path), *args, **kwargs)

def patched_json(self, path, *args, **kwargs):
    return _orig_json(self, translate_local_volumes(path), *args, **kwargs)

DataFrameReader.load = patched_load
DataFrameReader.csv = patched_csv
DataFrameReader.parquet = patched_parquet
DataFrameReader.json = patched_json

_orig_writer_save = DataFrameWriter.save
_orig_writer_csv = DataFrameWriter.csv
_orig_writer_parquet = DataFrameWriter.parquet
_orig_writer_json = DataFrameWriter.json
_orig_saveAsTable = DataFrameWriter.saveAsTable

def patched_writer_save(self, path=None, *args, **kwargs):
    if path is not None:
        path = translate_local_volumes(path)
    return _orig_writer_save(self, path, *args, **kwargs)

def patched_writer_csv(self, path, *args, **kwargs):
    return _orig_writer_csv(self, translate_local_volumes(path), *args, **kwargs)

def patched_writer_parquet(self, path, *args, **kwargs):
    return _orig_writer_parquet(self, translate_local_volumes(path), *args, **kwargs)

def patched_writer_json(self, path, *args, **kwargs):
    return _orig_writer_json(self, translate_local_volumes(path), *args, **kwargs)

def patched_saveAsTable(self, tableName, *args, **kwargs):
    clean_name = tableName
    for cat in ["274", "new"]:
        clean_name = clean_name.replace(f"`{cat}`.", "")
        clean_name = clean_name.replace(f"{cat}.", "")
    return _orig_saveAsTable(self, clean_name, *args, **kwargs)

DataFrameWriter.save = patched_writer_save
DataFrameWriter.csv = patched_writer_csv
DataFrameWriter.parquet = patched_writer_parquet
DataFrameWriter.json = patched_writer_json
DataFrameWriter.saveAsTable = patched_saveAsTable

# 5. Patch Spark Catalog to translate 3-part namespaces in tableExists check
from pyspark.sql.catalog import Catalog

_orig_tableExists = Catalog.tableExists

def patched_tableExists(self, tableName, dbName=None):
    clean_name = tableName
    for cat in ["274", "new"]:
        clean_name = clean_name.replace(f"`{cat}`.", "")
        clean_name = clean_name.replace(f"{cat}.", "")
    return _orig_tableExists(self, clean_name, dbName)

Catalog.tableExists = patched_tableExists
# ==============================================================================

class DBUtilsWidgets:
    def __init__(self):
        self.widgets = {}
        self.parameters = {}

    def text(self, name, default_value, label=""):
        if name not in self.widgets:
            self.widgets[name] = default_value

    def get(self, name):
        if name in self.parameters:
            return self.parameters[name]
        return self.widgets.get(name, "")

    def removeAll(self):
        self.widgets.clear()

class DBUtilsFS:
    def ls(self, path):
        local_path = self._translate_path(path)
        p = Path(local_path)
        if not p.exists():
            raise Exception(f"Path not found: {path}")
        
        if p.is_file():
            return [{
                "path": str(p.absolute()),
                "name": p.name,
                "size": p.stat().st_size
            }]
            
        results = []
        for child in p.iterdir():
            size = child.stat().st_size if child.is_file() else 0
            results.append({
                "path": str(child.absolute()),
                "name": child.name,
                "size": size
            })
        return results

    def cp(self, src, dest, recurse=False):
        src_local = self._translate_path(src)
        dest_local = self._translate_path(dest)
        Path(dest_local).parent.mkdir(parents=True, exist_ok=True)
        if recurse and Path(src_local).is_dir():
            shutil.copytree(src_local, dest_local, dirs_exist_ok=True)
        else:
            shutil.copy2(src_local, dest_local)
        return True

    def mv(self, src, dest, recurse=False):
        src_local = self._translate_path(src)
        dest_local = self._translate_path(dest)
        Path(dest_local).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src_local, dest_local)
        return True

    def _translate_path(self, path):
        if path.startswith("file:"):
            path = path[5:]
        if path == "/Volumes":
            resolved_absolute_path = CATALOG_ROOT / "bronze"
            resolved_absolute_path.mkdir(parents=True, exist_ok=True)
            return str(resolved_absolute_path.absolute())
        elif path.startswith("/Volumes/"):
            resolved_absolute_path = CATALOG_ROOT / "bronze" / path[9:]
            resolved_absolute_path.parent.mkdir(parents=True, exist_ok=True)
            return str(resolved_absolute_path.absolute())
        return path

# Mock classes for Databricks dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags() API
class MockContextTags:
    def get(self, name):
        class GetOrElse:
            def getOrElse(self, fallback_func):
                return fallback_func()
        return GetOrElse()

class MockContext:
    def tags(self):
        return MockContextTags()

class MockNotebookContext:
    def getContext(self):
        return MockContext()

class MockGetDbutils:
    def notebook(self):
        return MockNotebookContext()

class MockEntryPoint:
    def getDbutils(self):
        return MockGetDbutils()

class DBUtilsNotebook:
    def __init__(self):
        self.entry_point = MockEntryPoint()

    def run(self, path, timeout, arguments=None):
        print(f"\n[LOCAL EXECUTION] Running child notebook: {path}")
        print(f"Arguments: {arguments}")
        
        notebook_dir = Path(path).parent
        notebook_name = Path(path).name
        
        actual_path = Path(path)
        if not actual_path.exists():
            if Path(f"{path}.ipynb").exists():
                actual_path = Path(f"{path}.ipynb")
            else:
                for p in Path(".").glob(f"**/{notebook_name}.ipynb"):
                    actual_path = p
                    break
        
        if not actual_path.exists():
            raise FileNotFoundError(f"Could not locate child notebook: {path}")
            
        return run_notebook_locally(actual_path, arguments)

    def exit(self, val):
        dbutils.exit(val)

class DBUtilsMock:
    def __init__(self):
        self.widgets = DBUtilsWidgets()
        self.fs = DBUtilsFS()
        self.notebook = DBUtilsNotebook()

    def exit(self, val):
        print(f"[LOCAL EXIT] Notebook exited with response: {val}")
        self._exit_value = val
        raise NotebookExitException(val)

class NotebookExitException(Exception):
    def __init__(self, value):
        self.value = value

dbutils = DBUtilsMock()

def display(df, *args, **kwargs):
    if hasattr(df, "show"):
        df.show(truncate=False)
    else:
        print(df)

def run_notebook_locally(ipynb_path: Path, arguments=None) -> str:
    """Parses and runs a Databricks .ipynb notebook file locally in VSCode."""
    print(f"Loading notebook cells: {ipynb_path}")
    with open(ipynb_path, "r", encoding="utf-8") as f:
        nb = json.load(f)
        
    # Apply widget arguments (assigned to parameters so they survive widgets.removeAll())
    dbutils.widgets.parameters.clear()
    if arguments:
        for k, v in arguments.items():
            dbutils.widgets.parameters[k] = v
            
    # Inject variables into global scope
    global_dict = globals()
    global_dict['spark'] = spark
    global_dict['dbutils'] = dbutils
    global_dict['display'] = display
    global_dict['__name__'] = '__main__'
    
    exit_value = "SUCCESS"
    orig_cwd = os.getcwd()
    
    try:
        os.chdir(ipynb_path.absolute().parent)
        
        for idx, cell in enumerate(nb.get("cells", [])):
            if cell.get("cell_type") == "code":
                source = "".join(cell.get("source", []))
                
                stripped = source.strip()
                if not stripped or stripped.startswith("%pip"):
                    continue
                    
                if stripped.startswith("%run"):
                    run_path_str = stripped.split()[1].replace('"', '').replace("'", "")
                    run_notebook_path = Path(run_path_str)
                    if not run_notebook_path.suffix:
                        run_notebook_path = Path(f"{run_notebook_path}.ipynb")
                    
                    print(f"Executing %run magic: {run_notebook_path}")
                    run_notebook_locally(run_notebook_path)
                    continue
                    
                try:
                    code_obj = compile(source, f"{ipynb_path.name} - Cell {idx}", "exec")
                    exec(code_obj, global_dict)
                except NotebookExitException as e:
                    exit_value = e.value
                    break
                except Exception as e:
                    print(f"Error executing cell {idx} in {ipynb_path.name}: {e}")
                    raise
    finally:
        os.chdir(orig_cwd)
        
    return exit_value
