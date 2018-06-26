#include"sqlcommand.hpp"

string SqlCommand::get_file_name()
{
    return file_name;
}

void SqlCommand::set_file_name(string str)
{
    file_name = str;
}

string SqlCommand::get_col_name()
{
    return col_name;
}

void SqlCommand::set_col_name(string str)
{
    col_name = str;
}

string SqlCommand::get_index_name()
{
    return index_name;
}

void SqlCommand::set_index_name(string str)
{
    index_name = str;
}

string SqlCommand::get_database_name()
{
    return database_name;
}

void SqlCommand::set_database_name(string str)
{
    database_name = str;
}

string SqlCommand::get_table_name()
{
    return table_name;
}

void SqlCommand::set_table_name(string str)
{
    table_name = str;
}

int SqlCommand::get_command_type()
{
    return command_type;
}

void SqlCommand::set_command_type(int type)
{
    command_type = type;
}

vector<string> SqlCommand::get_col_name_vector()
{
    return col_name_vector;
}

void SqlCommand::push_col_name_vector(string str)
{
    col_name_vector.push_back(str);
}

vector<string> SqlCommand::get_col_values_vector()
{
    return col_values_vector;
}

void SqlCommand::push_col_values_vector(string str)
{
    col_values_vector.push_back(str);
}

vector<string> SqlCommand::get_command_left_vector()
{
    return command_left_vector;
}

void SqlCommand::push_command_left_vector(string str)
{
    command_left_vector.push_back(str);
}

vector<string> SqlCommand::get_command_op_vector()
{
    return command_op_vector;
}

void SqlCommand::push_command_op_vector(string str)
{
    command_op_vector.push_back(str);
}

vector<string> SqlCommand::get_command_right_vector()
{
    return command_right_vector;
}

void SqlCommand::push_command_right_vector(string str)
{
    command_right_vector.push_back(str);
}

vector<string> SqlCommand::get_col_special_vector()
{
    return col_special_vector;
}

void SqlCommand::push_col_special_vector(string str)
{
    col_special_vector.push_back(str);
}

void SqlCommand::push_value_type(int type)
{
    value_type.push_back(type);
}

vector<int> SqlCommand::get_value_type()
{
    return value_type;
}
