query = '''
SELECT
 t1.campo_string_1     AS CAMPO_STRING_1,
 t1.campo_string_2     AS CAMPO_STRING_2,
 t1.campo_string_3     AS CAMPO_STRING_3,
 t1.coluna_float_1     AS COLUNA_FLOAT_1,
 t1.coluna_float_2     AS COLUNA_FLOAT_2,
 t1.coluna_float_3     AS COLUNA_FLOAT_3,
 t1.coluna_date_yf_1   AS COLUNA_DATE_YF_1,
 t1.coluna_date_yf_2   AS COLUNA_DATE_YF_2,
 t1.coluna_date_yf_3   AS COLUNA_DATE_YF_3,
 t1.coluna_date_df_1   AS COLUNA_DATE_DF_1,
 t1.coluna_date_df_2   AS COLUNA_DATE_DF_2,
 t1.coluna_date_df_3   AS COLUNA_DATE_DF_3,
 t1.coluna_bool_1      AS COLUNA_BOOL_1,
 t1.coluna_bool_2      AS COLUNA_BOOL_2,
 t1.coluna_bool_3      AS COLUNA_BOOL_3,
 t1.coluna_integer_1    AS COLUNA_INTEGER_1,
 t1.coluna_integer_2    AS COLUNA_INTEGER_2,
 t1.coluna_integer_3    AS COLUNA_INTEGER_3
FROM  mariadb.tabela1 t1  
  WHERE 
  t1.campo_string_1 IS NOT NULL
ORDER BY 
  t1.coluna_date_yf_1 asc
'''
