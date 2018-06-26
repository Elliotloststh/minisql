#ifndef INTERPRETER_H
#define INTERPRETER_H

#include"sqlcommand.hpp"
#include<iostream>
#include<string>
#include<vector>
#include<algorithm>

using namespace std;

class InterPreter
{
public:
    string Read_input(); //���������ֵ
    string initial_sentense(string old_sql);
    string Process(string sql);
    string replace_allvalues(string sql, string old_values, string new_values);
    string get_first_word(string sql, string split);
    string del_first_word(string sql, string split);
    string split(string value);
    
    SqlCommand drop_database(string sql);
    SqlCommand drop_table(string sql);
    SqlCommand drop_index(string sql);
    SqlCommand quit();
    SqlCommand execfile(string sql);
    SqlCommand create_index(string sql);
    SqlCommand insert_values(string sql);
    SqlCommand delete_from(string sql);
    SqlCommand select_from(string sql);
    SqlCommand create_table(string sql);
    SqlCommand use_database(string sql);
    SqlCommand show_databases(string sql);
    SqlCommand show_tables(string sql);
    SqlCommand create_database(string sql);
    SqlCommand update_table(string sql);
    
    SqlCommand Final_expression(string sql); //�����������жϵ�����������ʽ
};

#endif // !INTERPRETER_H
