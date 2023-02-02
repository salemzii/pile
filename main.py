import os
import json
import xml.etree.ElementTree as ET
import csv
from pathlib import Path

class Pile():
    AcceptedFileExtIndex = ("csv", "json", "xml")
    MAX_FILE_SIZE_IN_BYTES = 2 * 1024 * 1024  # 2 MB
    FILECOLUMNINDEX = {}


    def __init__(self, path:str="", files = (), columns = ()) -> None:
        self.path = path
        self.files = files
        self.columns = columns

        self.matchFileColumns()

    def get_file_type(self, file: str):
        """
            returns the filetype of file a file by getting the extension
 
            Parameters
            ----------
            file: str
                Path to the file 
        """

        try:
            kind = lambda filename: str(filename).split(".")[-1]
            return kind(file)
        except Exception as err:
            print(f"Unexpected error: {err}")

    
    def extract_files(self,):
        
        """
            Compares a filetype against the supported file extensions and
            attempts to call the method for a specific filetype
        """
        fileLS = {}
        for file in self.files:
            
            t = self.get_file_type(file=file)
            if t in self.AcceptedFileExtIndex:
                try:
                    do = f"extract_{t}"
                    data, _ = self._call_next_func(file, nextFunc=do)
                    fileLS[file] = data

                except Exception as err:
                    print(err)
        return fileLS

    def extract_csv(self, file):

        """
            returns the rows and columns in a csv file 

            Parameters
            ----------
            csv_file: str
                Path to the csv_file 
        """

        data, columns = self._default_path_is_set(file, "_load_csv")
        return data, columns


    def extract_json(self, file):

        """
            returns the keys and values in a json file 

            Parameters
            ----------
            json_file: str
                Path to the json_file 
        """
        
        data, columns = self._default_path_is_set(file, "_load_json")
        return data, columns


    def extract_xml(self, file):

        """
            returns the keys and values in an xml file

            Parameters
            ----------
            xml_file: str
                Path to the json_file 
        """

        data, columns = self._default_path_is_set(file, "_load_xml")
        return data, columns


    def matchFileColumns(self):
        self.FILECOLUMNINDEX = dict(zip(self.files, self.columns))
        return self.FILECOLUMNINDEX
    

    def map_records_dep(self) -> list:
        '''
            map_records iterates through multiple data sources and
            attempts to match related records using columns that hold values
            common to all sources like firstname, sex and age; it then returns a 2-dimensional list 
            containing objects of related values
        '''
        _, json_data = self.extract_json()
        _, xml_data = self.extract_xml()
        _, csv_data = self.extract_csv()

        mapped_records = []
        for i in json_data:
            for j in xml_data:
                if i["firstName"] == j["firstName"]:
                    if i["lastName"] == j["lastName"]:
                        for k in csv_data:
                            if k["First Name"] == i["firstName"]:
                                if j["sex"].lower() == k["Sex"].lower() and j["age"] == k["Age (Years)"]:
                                    jsonXmlLs = [i, j, k]
                                    mapped_records.append(jsonXmlLs)

        return mapped_records
 
    def map_records(self) -> list:
        filerecordsIndex = self.extract_files()
        filesLen = len(filerecordsIndex)
        
        for c in range(0, filesLen):
            # do_something
            pass
        #print(filerecordsIndex, filesLen)



    def _default_path_is_set(self, file, nextFunc):

        """
            joins a file with it's path if path is set
        """
        if self.path != "":
            filepath = os.path.join(self.path, file)
            return self._call_next_func(file=filepath, nextFunc=nextFunc)
            # add error handling
        else:
            self._call_next_func(file=file, nextFunc=nextFunc)

    def _call_next_func(self, file, nextFunc):
        if hasattr(self, nextFunc) and callable(func := getattr(self, nextFunc)):
            return func(file=file)       


    def _load_xml(self, file):
        print(f"Loading xml: {file}")

        dataset = []
        if self._get_fs(file) <= self.MAX_FILE_SIZE_IN_BYTES:
            try:
                tree = ET.parse(file)

                # access the root node of the xml tree
                root = tree.getroot()
                columns = root[0].keys()
                
                for i in range(len(root)):
                    dataset.append(dict(root[i].items()))
                
                        
                return dataset, columns
            except Exception as err:
                print(f"error extracting {file}", err)
                return err



    def _load_json(self, file):
        print(f"Loading JSON: {file}")
        data = any

        if self._get_fs(file) <= self.MAX_FILE_SIZE_IN_BYTES:
            try:
                with open(file=file) as jsonfile:
                    data = json.load(jsonfile)
                    # fetch the keys of the json data, by accessing the first object
                    columns = [i for i in data[0]]
                return data, columns
            except Exception as err:
                print(f"error extracting {file}", err)
                return err


    def _load_csv(self, file):

        print(f"Loading CSV: {file}")
        dataset = []
        if self._get_fs(file) <= self.MAX_FILE_SIZE_IN_BYTES:
            try:
                with open(file, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    # the next function counts all the keys in the given iterable
                    columns = next(reader)

                    
                    rows = list()
                    # extracting each data row one by one
                    for row in reader:
                        rows.append(row)

                        # allocate the column values (i. e keys) of the given csv file to each row from the list
                        dt = dict(zip(columns, row))
                        dataset.append(dt)
                return dataset, columns                
            except Exception as err:
                print(f"error extracting {file}", err) 
                return err
        else:
            raise        


    def _get_fs(self, path = Path('.')) -> int:
        filepath = Path(path)
        if filepath.is_file():
            fs = filepath.stat().st_size
            return fs


p = Pile(path="/home/cygman24/projects/pile/data", 
    files = ("user_data.csv",
            "user_data.xml", 
            "user_data.json"
    ), 
    columns = (
        ("First Name", "Second Name", "Age (Years)"), 
        ("firstName", "lastName", "age"),
        ("firstName", "lastName", "age")
    )
)
 
(p.map_records())
print(p.FILECOLUMNINDEX)