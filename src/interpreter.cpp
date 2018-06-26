#include"interpreter.hpp"

/*读入之后进行的预处理*/
string InterPreter::Read_input()
{
    string sql = ""; //初始语句
    string temp;
    
    while (1)
    {
        cin >> temp; //读入输入的一列
        sql += temp + " "; //预处理为空格形式
        
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

/*主要是为了从file里面获取sql语句*/
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

/*预处理模块*/
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

/*替代值的函数*/
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

/*获取到第一个word*/
string InterPreter::get_first_word(string sql, string split)
{
    int start_position = sql.find_first_not_of(split);
    if (start_position < 0)
        return "";
    int end_position = sql.find_first_of(split);
    if (end_position < 0)
        return sql.substr(start_position, split.length()); //如果没有找到的话，返回最后一个符号，一般为分号结尾
    
    string temp = sql.substr(start_position, end_position - start_position);
    //transform(temp.begin(), temp.end(), temp.begin(), ::tolower); //转换为小写之后输出
    
    return temp;
}

/*删除掉第一个word之后的sql字符串*/
string InterPreter::del_first_word(string sql, string split)
{
    int start_position = sql.find_first_of(split);
    start_position++;
    sql = sql.substr(start_position);
    
    return sql;
}

/*测试的是drop database A;*/
SqlCommand InterPreter::drop_database(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string attr = get_first_word(sql, " ");
    
    if (attr == ";") //未输入数据库名
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_database_name(attr);
    temp.set_command_type(SQL_DROP_DATABASE);
    return temp;
}

/*测试的是drop table A;*/
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

/*测试的是drop index A;*/
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

/*测试的是 quit;*/
SqlCommand InterPreter::quit()
{
    SqlCommand temp;
    temp.set_command_type(SQL_QUIT);
    
    return temp;
}

/*测试的是 execfile A;*/
SqlCommand InterPreter::execfile(string sql)
{
    SqlCommand temp;
    
    sql = del_first_word(sql, " "); //删除空格及空格之前的execfile
    string filename = get_first_word(sql, " ");
    
    if (filename == ";")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    temp.set_file_name(filename); //设置为需要执行的sql文件
    temp.set_command_type(SQL_EXECFILE);
    return temp;
}

/*测试的是 create index A(index_name) on B(表名) (C) ->属性名;*/
SqlCommand InterPreter::create_index(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " "); //删掉create
    sql = del_first_word(sql, " "); //删掉index
    
    string index_name = get_first_word(sql, " ");
    
    sql = del_first_word(sql, " "); //删掉index_name
    
    /*判断是否为 on*/
    if (get_first_word(sql, " ") != "on")
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " "); //如果是on的话删掉继续向下
    
    string table_name = get_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    /*判断是否符合()形式*/
    int start_dis = sql.find("(");
    int end_dis = sql.find(")");
    
    if (start_dis == -1 || end_dis == -1 || (end_dis < start_dis)) //形式有问题，直接返回
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    /*得到属性名*/
    int start = sql.find_first_not_of("()");
    int end = sql.find_last_of("()");
    string attribute_name = sql.substr(start, end - start);
    
    
    temp.set_index_name(index_name);
    temp.set_table_name(table_name);
    temp.set_col_name(attribute_name);
    temp.set_command_type(SQL_CREATE_INDEX);
    
    return temp;
}

/*测试的是 insert into table values ('A', 'B', 'C' ...) ;*/
SqlCommand InterPreter::insert_values(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (get_first_word(sql, " ") != "into") //不是into，直接报错
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

/*测试的是delete from t1 where id < 4 and salary > 3000.00; 格式限定...*/
SqlCommand InterPreter::delete_from(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (get_first_word(sql, " ") != "from") //不是from，直接报错
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " "); //跳过delete 和 from
    
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name);
    sql = del_first_word(sql, " "); //跳过table这里
    
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
            sql = del_first_word(sql, " ");             //这部分主要处理掉了条件里面的左值，右值和中间的比较值
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_DELETE_FROM);
    return temp;
}

/*测试的是 select * from student where sage > 20 and sgender = 'F'; 都是select* 类型*/
SqlCommand InterPreter::select_from(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " ");
    
    if (sql.find("*") == -1 || sql.find("from") == -1 || sql.find("from") < sql.find("*")) //对于* 和 from有错误的话，直接报错
    {
        temp.set_command_type(SQL_ERROR);
        return temp;
    }
    
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    string table_name = get_first_word(sql, " ");
    temp.set_table_name(table_name); //表的名字
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
            sql = del_first_word(sql, " ");             //这部分主要处理掉了条件里面的左值，右值和中间的比较值
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_SELECT_FROM);
    return temp;
}

/*接下来处理的建表 create table student (sno char(8),sname char(16) unique,sage int,primary key (sno)) ;*/
SqlCommand InterPreter::create_table(string sql)
{
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    
    SqlCommand temp;
    string table_name = get_first_word(sql, " (");
    temp.set_table_name(table_name); //表的名字
    sql = del_first_word(sql, "("); //保留下字段的信息
    
    while (get_first_word(sql, " ;").length() != 0) //一直读到末尾
    {
        string expression = get_first_word(sql, ",;");
        if (get_first_word(expression, " ") == "primary") //读到最后一条语句是primary key的时候
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
            break; //语句全部处理完毕，退出循环
        }
        else
        {
            string attribute = get_first_word(expression, " ");
            temp.push_col_name_vector(attribute);
            
            expression = del_first_word(expression, " ");
            
            /*判断是否为unique*/
            if (expression.find("unique") != -1)
                temp.push_col_special_vector("unique");
            else
                temp.push_col_special_vector("null");
            
            /*判断值类型*/
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
                temp.push_value_type(value); //把value值放进去代表当前的类型
            }
            else //如果一项都没有匹配到，直接error
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

/*处理的是use database ;*/
SqlCommand InterPreter::use_database(string sql)
{
    SqlCommand temp;
    sql = del_first_word(sql, " "); //跳过第一个use
    
    string database_name = get_first_word(sql, " ");
    temp.set_database_name(database_name); //设置数据库的名称
    temp.set_command_type(SQL_USE_DATABASE); //设置命令的类型为use database
    
    sql = del_first_word(sql, " ");
    if (sql != ";")
        temp.set_command_type(SQL_ERROR);
    return temp;
}

/*处理的是show database;*/
SqlCommand InterPreter::show_databases(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_SHOW_DATABASES); //设置命令的类型为use database
    
    return temp;
}

/*处理的是show tables;*/
SqlCommand InterPreter::show_tables(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_SHOW_TABLES); //设置命令的类型为use database
    
    return temp;
}

/*处理的是 create database A;*/
SqlCommand InterPreter::create_database(string sql)
{
    SqlCommand temp;
    temp.set_command_type(SQL_CREATE_DATABASE); //设置命令的类型为use database
    
    sql = del_first_word(sql, " ");
    sql = del_first_word(sql, " ");
    string database_name = get_first_word(sql, " ");
    temp.set_database_name(database_name);
    
    return temp;
}

/*处理的是 update table set id = 5 where ... and ....;*/
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
            sql = del_first_word(sql, " ");             //这部分主要处理掉了条件里面的左值，右值和中间的比较值
            
            if (get_first_word(sql, " ") == "and")
                sql = del_first_word(sql, " ");
        }
    }
    
    temp.set_command_type(SQL_UPDATE);
    return temp;
}

/*判断选择哪种类型*/
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
