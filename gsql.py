import pymysql.cursors
import pandas as pd
class gsql():
    def __init__(self,token):
        self.token = token
        self.token['cursorclass'] = pymysql.cursors.DictCursor
        self.connect = pymysql.connect(**self.token)
        
    def get(self, query):
        
        
        with self.connect.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            field_names = [i[0] for i in cursor.description]
            cursor.close()
        db_data_df = pd.DataFrame(result,columns = field_names)
        
        return db_data_df
    
    def put(self, query):
        with self.connect.cursor() as cursor:
            cursor.execute(query)
        self.connect.commit()
        return True
    
    def close(self):
        self.connect.close()
        
    def reopen(self):
        self.connect = pymysql.connect(**self.token)
        
    def creat_table_q(self, name, fields):
        base = f"CREATE TABLE {name} ("
        for name, fls_type in fields.items():
            column = f"{name} {fls_type}, "
            base += column
        base =  base[:-2] 
        base += ")"
        return base
    def sqlise_value(self, j):
        if type(j) == float or type(j) == int:
            val=str(int(j))
        elif type(j) == str:
            val="'"+j+"'"
        else:
            val="'"+str(j)+"'"
        return val

    def insert_pd(self, df,table, columns = []):
        base = f"INSERT INTO {table} "
        columns = df.columns if columns == [] else columns
        base += f'({", ".join(columns)}) VALUES '
        for i in df.itertuples():
            vals = []
            for j in i[1:]:
                val = self.sqlise_value(j)
                vals.append(val)  
            row = f'({", ".join(vals)}),'
            base += row

        return base[:-1]

    def update_pd(self, df, table, cases, columns = []):
        queries= []
        base  = f"UPDATE {table} SET "
        columns = df.columns if columns == [] else columns
        for value in df.itertuples():
            vals = []
            c = 0
            for j in value[1:]:
                val = self.sqlise_value(j)
                vals.append(f"{columns[c]}={val}")
                c += 1
            row = f'{", ".join(vals)}'
            case = f""" WHERE {cases[0]}={cases[1]};"""
            query = base+row+case
            queries.append(query)
        return queries
    
    def update_df_q(self, df_changes, table):
        queries = []
        base  = f"UPDATE {table} SET "
        for i in df_changes.itertuples():
            vals = f"{i.changed_cols} = {i.changed_values}"
            case = f"  WHERE {i.case_cols} = {i.case_vals}"
            query = base+vals+case
            queries.append(query)
        return queries