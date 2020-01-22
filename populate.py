import psycopg2
import csv

conn = psycopg2.connect(host='ds-league.inf.udec.cl',
                        port='5432',
                        dbname='app',
                        user='postgres',
                        password='partialpass')
cur = conn.cursor()
print('DB successfully connected!')
with open('Gastos.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    query = "INSERT INTO core_expense(g, ingreso, c1,\
                                      c2, c3, c4, c5, c6,\
                                      c7, c8, c9, c10, c11,\
                                      c12, ahorro_deuda) \
                                      VALUES(%s, %s, %s,\
                                             %s, %s, %s, %s, %s,\
                                             %s, %s, %s, %s, %s,\
                                             %s, %s);"
    cur.execute("TRUNCATE TABLE core_expense RESTART IDENTITY;")
    for row in reader:
        cur.execute(query, row)
    print('init commit')
    conn.commit()

print('Expense table completed!')
print('Starting People Table')
with open('Personas.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)

    query = "INSERT INTO core_people(folio, inga_hd, fe,\
                                      persona, npersonas, edad, \
                                      ingreso_hd, ingreso_pc) \
                                      VALUES(%s, %s, %s,\
                                             %s, %s, %s, %s, %s);"
    cur.execute("TRUNCATE TABLE core_people RESTART IDENTITY;")
    for row in reader:
        cur.execute(query, row)
    print('init commit')
    conn.commit()
print('People table completed!')
print('Starting Family Table')

with open('Familias.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)

    query = "INSERT INTO core_family(index_exp, index, folio,\
                                      ingreso, fe, np, \
                                      ingreso_disp, gasto, ptge_gasto) \
                                      VALUES(%s, %s, %s,\
                                             %s, %s, %s, \
                                             %s, %s, %s);"
    cur.execute("TRUNCATE TABLE core_family RESTART IDENTITY;")
    for row in reader:
        new_row = row[0:5] + row[6:]
        cur.execute(query, new_row)
    print('init commit')
    conn.commit()
