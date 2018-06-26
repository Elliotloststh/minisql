#include"interpreter.hpp"

/*����֮����е�Ԥ����*/
string InterPreter::Read_input()
{
    string sql = ""; //��ʼ���
    string temp;
    
    while (1)
    {
        cin >> temp; //���������һ��
        sql += temp + " "; //Ԥ����Ϊ�ո���ʽ
        
        if (sql.at(sql.length() - 2) == ';')
        {
            sql = sql.substr(0, sql.length() - 2);
            sql += " ;";
            break;
        }
    }
    
    sql = Process(sql);
    return sql;
}

/*��Ҫ��Ϊ�˴�file�����ȡsql���*/
string InterPreter::initial_sentense(string old_sql)
{
    if (old_sql.at(old_sql.length() - 1) == ';')
    {
        old_sql = old_sql.substr(0, old_sql.length() - 1);
        old_sql += " ;";
        old_sql = Process(old_sql);
        return old_sql;
    }
    
    return "error";
}

/*Ԥ����ģ��*/
string InterPreter::Process(string sql)
{
    sql = replace_allvalues(sql, "( ", "(");
    sql = replace_allvalues(sql, " )", ")");
    //sql = replace_allvalues(sql, ") ", ")");
    sql = replace_allvalues(sql, ", ", ",");
    sql = replace_allvalues(sql, " ,", ",");
    sql = replace_allvalues(sql, "  ", " ");
    return sql;
}

/*���ֵ�ĺ���*/
string InterPreter::replace_allvalues(string sql, const string old_values, const string new_values)
{
    while (1)
    {
        if (sql.find(old_values) == -1)
            break;
        else
        {
            int position = sql.find(old_values);
            sql.replace(position, old_values.length(), new_values);
        }
    }
    
    return sql;
}

/*��ȡ����һ��word*/
string InterPreter::get_first_word(string sql, string split)
{
    int start_position = sql.find_first_not_of(split);
    if (start_position < 0)
        return "";
    int end_position = sql.find_first_of(split);
    if (end_position < 0)
        return sql.substr(start_position, split.length()); //���û���ҵ��Ļ����������һ�����ţ�һ��Ϊ�ֺŽ�β
    
    string temp = sql.substr(start_position, end_position - start_position);
    //transform(temp.begin(), temp.end(), temp.begin(), ::tolower); //ת��ΪСд֮�����
    
    return temp;
}

/*ɾ������һ��word֮���sql�ַ���*/
string InterPreter::del_first_word(string sql, string split)
{
    int start_position = sql.find_first_of(split);
    start_position++;
    sql = sql.substr(start_position);
    
    return sql;
}

/*���Ե���drop database A;*/
SqlCommand InterPreter::drop_database(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string attr = get_first_word(sql, " ");
    
    if (attr == ";") //δ�������ݿ���
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_database_name(attr);
    temp.set_command_type(SQL_DROP_DATABASE);
    return temp;
}

/*���Ե���drop table A;*/
SqlCommand InterPreter::drop_table(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string attr = get_first_word(sql, " ");
    
    if (attr == ";")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_table_name(attr);
    temp.set_command_type(SQL_DROP_TABLE);
    return temp;
}

/*���Ե���drop index A;*/
SqlCommand InterPreter::drop_index(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string attr = get_first_word(sql, " ");
    
    if (attr == ";")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_index_name(attr);
    temp.set_command_type(SQL_DROP_INDEX);
    return temp;
}

/*���Ե��� quit;*/
SqlCommand InterPreter::quit()
{
    SqlCommand temp;
    temp.set_command_type(SQL_QUIT);
    
    return temp;
}

/*���Ե��� execfile A;*/
SqlCommand InterPreter::execfile(string sql)
{
    SqlCommand temp;
    
    sql = del_first_word(sql, " "); //ɾ���ո񼰿ո�֮ǰ��execfile
    string filename = get_first_word(sql, " ");
    
    if (filename == ";")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_file_name(filename); //����Ϊ��Ҫִ�е�sql�ļ�
    temp.set_command_type(SQL_EXECFILE);
    return temp;
}

/*���Ե��� create index A(index_name) on B(����) (C) ->������;*/
SqlCommand InterPreter::create_index(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " "); //ɾ��create
    sql = del_first_word(sql, " "); //ɾ��index
    
    string index_name = get_first_word(sql, " ");
    
    sql = del_first_word(sql, " "); //ɾ��index_name
    
    /*�ж��Ƿ�Ϊ on*/
    if (get_first_word(sql, " ") != "on")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " "); //�����on�Ļ�ɾ����������
    
    string table_name = get_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    /*�ж��Ƿ����()��ʽ*/
    int start_dis = sql.find("(");
    int end_dis = sql.find(")");
    
    if (start_dis == -1 || end_dis == -1 || (end_dis < start_dis)) //��ʽ�����⣬ֱ�ӷ���
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    /*�õ�������*/
    int start = sql.find_first_not_of("()");
    int end = sql.find_last_of("()");
    string attribute_name = sql.substr(start, end - start);
    
    
    temp.set_index_name(index_name);
    temp.set_table_name(table_name);
    temp.set_col_name(attribute_name);
    temp.set_command_type(SQL_CREATE_INDEX);
    
    return temp;
}

/*���Ե��� insert into table values ('A', 'B', 'C' ...) ;*/
SqlCommand InterPreter::insert_values(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (get_first_word(sql, " ") != "into") //����into��ֱ�ӱ���
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " ");
    
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name);
    sql = del_first_word(sql, "(");
    
    while (get_first_word(sql, " )") != ";")
    {
        string value = get_first_word(sql, " ),");
        value = split(value);
        temp.push_col_values_vector(value);
        sql = del_first_word(sql, ",)");
    }
    
    temp.set_command_type(SQL_INSERT_INTO);
    return temp;
}

/*���Ե���delete from t1 where id < 4 and salary > 3000.00; ��ʽ�޶�...*/
SqlCommand InterPreter::delete_from(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (get_first_word(sql, " ") != "from") //����from��ֱ�ӱ���
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " "); //����delete �� from
    
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name);
    sql = del_first_word(sql, " "); //����table����
    
    if (get_first_word(sql, " ") == "where")
    {
        sql = del_first_word(sql, " ");
        while (get_first_word(sql, " ") != ";")
        {
            string left_value = get_first_word(sql, " ");
            temp.push_command_left_vector(left_value);
            sql = del_first_word(sql, " ");
            
            string op_value = get_first_word(sql, " ");
            temp.push_command_op_vector(op_value);
            sql = del_first_word(sql, " ");
            
            string right_value = get_first_word(sql, " ");
            right_value = split(right_value);
            temp.push_command_right_vector(right_value);
            sql = del_first_word(sql, " ");             //�ⲿ����Ҫ������������������ֵ����ֵ���м�ıȽ�ֵ
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_DELETE_FROM);
    return temp;
}

/*���Ե��� select * from student where sage > 20 and sgender = 'F'; ����select* ����*/
SqlCommand InterPreter::select_from(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (sql.find("*") == -1 || sql.find("from") == -1 || sql.find("from") < sql.find("*")) //����* �� from�д���Ļ���ֱ�ӱ���
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name); //�������
    sql = del_first_word(sql, " ");
    
    if (get_first_word(sql, " ") == "where")
    {
        sql = del_first_word(sql, " ");
        while (get_first_word(sql, " ") != ";")
        {
            string left_value = get_first_word(sql, " ");
            temp.push_command_left_vector(left_value);
            sql = del_first_word(sql, " ");
            
            string op_value = get_first_word(sql, " ");
            temp.push_command_op_vector(op_value);
            sql = del_first_word(sql, " ");
            
            string right_value = get_first_word(sql, " ");
            right_value = split(right_value);
            temp.push_command_right_vector(right_value);
            sql = del_first_word(sql, " ");             //�ⲿ����Ҫ������������������ֵ����ֵ���м�ıȽ�ֵ
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_SELECT_FROM);
    return temp;
}

/*����������Ľ��� create table student (sno char(8),sname char(16) unique,sage int,primary key (sno)) ;*/
SqlCommand InterPreter::create_table(string sql)
{
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    SqlCommand temp;
    string table_name = get_first_word(sql, " (");
    temp.set_table_name(table_name); //�������
    sql = del_first_word(sql, "("); //�������ֶε���Ϣ
    
    while (get_first_word(sql, " ;").length() != 0) //һֱ����ĩβ
    {
        string expression = get_first_word(sql, ",;");
        if (get_first_word(expression, " ") == "primary") //�������һ�������primary key��ʱ��
        {
            int i;
            int start = expression.find("(");
            int end = expression.find(")");
            string attri = expression.substr(start + 1, end - start - 1);
            
            for (i = 0; i < temp.col_name_vector.size(); i++)
            {
                if (attri == temp.col_name_vector[i])
                    break;
            }
            
            temp.col_special_vector[i] = "primary";
            break; //���ȫ��������ϣ��˳�ѭ��
        }
        else
        {
            string attribute = get_first_word(expression, " ");
            temp.push_col_name_vector(attribute);
            
            expression = del_first_word(expression, " ");
            
            /*�ж��Ƿ�Ϊunique*/
            if (expression.find("unique") != -1)
                temp.push_col_special_vector("unique");
            else
                temp.push_col_special_vector("null");
            
            /*�ж�ֵ����*/
            if (expression.find("int") == 0)
                temp.push_value_type(0);
            else if (expression.find("float") == 0)
                temp.push_value_type(-1);
            else if (expression.find("char") == 0)
            {
                expression = del_first_word(expression, "(");
                int start = expression.find_first_not_of("(");
                int end = expression.find_first_of(")");
                string type_value = expression.substr(start, end - start);
                int value = stoi(type_value);
                temp.push_value_type(value); //��valueֵ�Ž�ȥ����ǰ������
            }
            else //���һ�û��ƥ�䵽��ֱ��error
            {
                temp.set_command_type(SQL_ERROR);
                return temp;
            }
        }
        
        sql = del_first_word(sql, ",");
    }
    
    temp.set_command_type(SQL_CREATE_TABLE);
    return temp;
}

/*�������use database ;*/
SqlCommand InterPreter::use_database(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " "); //������һ��use
    
    string database_name = get_first_word(sql, " ");
    temp.set_database_name(database_name); //�������ݿ������
    temp.set_command_type(SQL_USE_DATABASE); //�������������Ϊuse database
    
    sql = del_first_word(sql, " ");
    if (sql != ";")
        temp.set_command_type(SQL_ERROR);
    return temp;
}

/*�������show database;*/
SqlCommand InterPreter::show_databases(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_SHOW_DATABASES); //�������������Ϊuse database
    
    return temp;
}

/*�������show tables;*/
SqlCommand InterPreter::show_tables(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_SHOW_TABLES); //�������������Ϊuse database
    
    return temp;
}

/*������� create database A;*/
SqlCommand InterPreter::create_database(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_CREATE_DATABASE); //�������������Ϊuse database
    
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string database_name = get_first_word(sql, " ");
    temp.set_database_name(database_name);
    
    return temp;
}

/*������� update table set id = 5 where ... and ....;*/
SqlCommand InterPreter::update_table(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name);
    
    sql = del_first_word(sql, " "); //skip table
    sql = del_first_word(sql, " "); //skip set
    
    string attr_name = get_first_word(sql, " ");
    temp.set_col_name(attr_name);
    
    sql = del_first_word(sql, " "); //skip id
    sql = del_first_word(sql, " "); //skip =
    
    string value = get_first_word(sql, " ");
    value = split(value);
    temp.set_col_value(value); //put the string value
    
    sql = del_first_word(sql, " "); //delete col_value
    
    if (get_first_word(sql, " ") == "where")
    {
        sql = del_first_word(sql, " ");
        while (get_first_word(sql, " ") != ";")
        {
            string left_value = get_first_word(sql, " ");
            temp.push_command_left_vector(left_value);
            sql = del_first_word(sql, " ");
            
            string op_value = get_first_word(sql, " ");
            temp.push_command_op_vector(op_value);
            sql = del_first_word(sql, " ");
            
            string right_value = get_first_word(sql, " ");
            right_value = split(right_value);
            temp.push_command_right_vector(right_value);
            sql = del_first_word(sql, " ");             //�ⲿ����Ҫ������������������ֵ����ֵ���м�ıȽ�ֵ
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_UPDATE);
    return temp;
}

/*�ж�ѡ����������*/
SqlCommand InterPreter::Final_expression(string sql)
{
    string first_word = get_first_word(sql, " ");
    SqlCommand temp;
    
    if (first_word == "quit")
        return quit();
    else if (first_word == "execfile")
        return execfile(sql);
    else if (first_word == "insert")
        return insert_values(sql);
    else if (first_word == "select")
        return select_from(sql);
    else if (first_word == "delete")
        return delete_from(sql);
    else if (first_word == "use")
        return use_database(sql);
    else if (first_word == "update")
        return update_table(sql);
    else if (first_word == "create")
    {
        if (sql.find("index") != -1)
            return create_index(sql);
        else if (sql.find("table") != -1)
            return create_table(sql);
        else if (sql.find("database") != -1)
            return create_database(sql);
        else
            temp.set_command_type(SQL_ERROR);
        return temp;
    }
    else if (first_word == "drop")
    {
        if (sql.find("index") != -1)
            return drop_index(sql);
        else if (sql.find("table") != -1)
            return drop_table(sql);
        else if (sql.find("database") != -1)
            return drop_database(sql);
        else
            temp.set_command_type(SQL_ERROR);
        return temp;
    }
    else if (first_word == "show")
    {
        if (sql.find("tables") != -1)
            return show_tables(sql);
        else if (sql.find("databases") != -1)
            return show_databases(sql);
        else
            temp.set_command_type(SQL_ERROR);
        return temp;
    }
    else
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
}

string InterPreter::split(string value)
{
    if (value.find('"') == 0 || value.find("'") == 0)
    {
        value = value.substr(1, value.length() - 2);
    }
    
    return value;
}
