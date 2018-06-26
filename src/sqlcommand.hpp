#ifndef SQLCOMMAND_H
#define SQLCOMMAND_H

#include<iostream>
#include<string>
#include<vector>

using namespace std;

#define SQL_DROP_DATABASE 1000
#define SQL_ERROR         1001
#define SQL_DROP_TABLE    1002
#define SQL_DROP_INDEX    1003
#define SQL_QUIT          1004
#define SQL_EXECFILE      1005
#define SQL_CREATE_INDEX  1006
#define SQL_INSERT_INTO   1007
#define SQL_DELETE_FROM   1008
#define SQL_SELECT_FROM   1009
#define SQL_CREATE_TABLE  1010
#define SQL_USE_DATABASE  1011
#define SQL_SHOW_DATABASES 1012
#define SQL_SHOW_TABLES   1013
#define SQL_CREATE_DATABASE 1014
#define SQL_UPDATE        1015

class SqlCommand
{
public:
    
    vector<string> col_name_vector; //属性名容器
    vector<string> col_values_vector; //属性值容器
    vector<string> command_left_vector; //where语句的左边属性名
    vector<string> command_op_vector; //where语句中间的操作符 = <> < > <= >=
    vector<string> command_right_vector; //where语句右边的属性值
    vector<string> col_special_vector; //判断是否有特殊约束,unique or primary key or null
    vector<int> value_type; //0代表整型, -1代表小数, 1-255代表的是char型的长度
    
    void push_value_type(int type);
    vector<int> get_value_type();
    
    void push_col_special_vector(string str);
    vector<string> get_col_special_vector();
    
    void push_command_right_vector(string str);
    vector<string> get_command_right_vector();
    
    void push_command_op_vector(string str);
    vector<string> get_command_op_vector();
    
    void push_command_left_vector(string str);
    vector<string> get_command_left_vector();
    
    void push_col_values_vector(string str);
    vector<string> get_col_values_vector();
    
    void push_col_name_vector(string str);
    vector<string> get_col_name_vector();
    
    void set_command_type(int type);
    int get_command_type();
    
    void set_table_name(string str);
    string get_table_name();
    
    void set_database_name(string str);
    string get_database_name();
    
    void set_col_name(string str);
    string get_col_name();
    
    void set_index_name(string str);
    string get_index_name();
    
    void set_file_name(string str);
    string get_file_name();
    
    void set_col_value(string str) { col_value = str; }
    string get_col_value() { return col_value; }
private:
    int command_type; //命令的类型
    string table_name; //表名
    string database_name; //数据库名
    string col_name; //属性名
    string col_value; //update时候用 属性值
    string index_name; //索引名
    string file_name; //执行execfile时的sql文件名
};

#endif // !SQLCOMMAND_H

