#include "API.hpp"
#include "interpreter.hpp"

extern CatalogManager CL;
extern IndexManager IM;
extern RecordManager RM;

using namespace std;

API::API(SqlCommand c)
{
    sql = c;
}

API::~API()
{
}

void API::execute(SqlCommand sql)
{
    SqlCommand sql_temp = sql; //临时定义的sql属性
    attr.clear(); con.clear(); attr_special.clear(); attr_normal.clear(); con_special.clear(); con_normal.clear();
    try {
        switch (sql_temp.get_command_type())
        {
            case (SQL_ERROR):
            {
                cout << "---- Syntax Error! ---- \n" << endl;
                break;
            }
                
            case (SQL_DROP_DATABASE):
            {
                CL.Drop_Database(sql_temp.get_database_name());
                //cout << "Query OK, 0 rows affected" << endl;
                break;
            }
                
            case (SQL_DROP_TABLE):
            {
                CL.Drop_Table(sql_temp.get_table_name());
                //cout << "Query OK, 0 rows affected" << endl;
                break;
            }
                
            case (SQL_DROP_INDEX):
            {
                CL.Drop_Index(sql_temp.get_index_name());
                //cout << "Query OK, 0 rows affected" << endl;
                break;
            }
                
            case (SQL_QUIT):
            {
                cout << "Bye!\n" << endl;
                exit(0);
            }
                
            case (SQL_EXECFILE):
            {
                sql_execfile(sql_temp);
                break;
            }
                
            case (SQL_CREATE_INDEX):
            {
                bool flag = true;
                flag = CL.Judge_Table_Exist(sql_temp.get_table_name());
                
                if (flag == false)
                {
                    cout << "No this table!!" << endl;
                    return;
                }
                
                flag = CL.Judge_Index(sql_temp.get_index_name(), sql_temp.get_table_name());
                
                if(flag == false)
                {
                    cout << "Can not create index!!" << endl;
                    return;
                }
                
                CL.Create_Index(sql_temp.get_index_name(), sql_temp.get_col_name(), CL.Get_Used_Database());
                //cout << "Query OK, 0 rows affected" << endl;
                break;
            }
                
                /*insert into table values ('A', 'B', 'C' ...)*/
            case (SQL_INSERT_INTO):
            {
                bool flag = true;
                flag = CL.Judge_Table_Exist(sql_temp.get_table_name());
                
                if (flag == false)
                {
                    cout << "This table does not exists!!" << endl;
                    return;
                }
                
                flag = CL.Judge_Data(sql_temp.col_values_vector, sql_temp.get_table_name());
                if (flag == false)
                {
                    cout << "Data Unvalid!!" << endl;
                    return;
                }
                
                /*All valid*/
                attr = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                int t = RM.Record_InsertValue(sql_temp.get_table_name(), sql_temp.col_values_vector, attr);
                if(t == -1)
                    cout<<"Duplicate entry for primary key or unique key"<<endl;
                else
                {
                    CL.Add_Tuple(1, sql_temp.get_table_name());
                    cout << "Query OK, 1 row affected" << endl;
                }
                break;
            }
                
            case (SQL_DELETE_FROM):
            {
                int record_num;
                bool flag = true;
                flag = CL.Judge_Table_Exist(sql_temp.get_table_name());
                
                if (flag == false)
                {
                    cout << "This table does not exists!!" << endl;
                    return;
                }
                
                /*delete from table 属性名，数据类型，数据长度，属性类型，索引名*/
                if (sql_temp.command_op_vector.size() == 0)
                {
                    attr = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                    record_num = RM.Record_DeleteAllValue(sql_temp.get_table_name(), attr);
                }
                else // delete from table where ...
                {
                    /*create condition vector*/
                    for (int i = 0; i < sql_temp.command_op_vector.size(); i++)
                    {
                        string name = sql_temp.command_left_vector[i];
                        string op = sql_temp.command_op_vector[i];
                        int id;
                        
                        if (op == "=") id = 0;
                        else if (op == "<>") id = 1;
                        else if (op == "<") id = 2;
                        else if (op == ">") id = 3;
                        else if (op == "<=") id = 4;
                        else if (op == ">=") id = 5;
                        
                        string value = sql_temp.command_right_vector[i];
                        Condition con_d(name, value, id);
                        con.push_back(con_d);
                    }
                    attr = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                    record_num = RM.Record_DeleteValue(sql_temp.get_table_name(), con, attr);
                }
                CL.Delete_Tuple(record_num, sql_temp.get_table_name());
                string info = "";
                info = info + "Query OK, " + to_string(record_num) + " rows affected";
                cout << info << endl;
                break;
            }
                
                /*select * from student where sage > 20 and sgender = 'F';*/
            case (SQL_SELECT_FROM):
            {
                SelectResult result;
                vector<Attribute> all_info;
                vector<int> Result;
                int record_num;
                bool flag = true;
                flag = CL.Judge_Table_Exist(sql_temp.get_table_name());
                if (flag == false)
                {
                    cout << "This table does not exists!!" << endl;
                    return;
                }
                
                /*select * from table ;*/
                if (sql_temp.command_op_vector.size() == 0)
                {
                    all_info = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                    record_num = RM.Record_SelectAll(sql_temp.get_table_name(), all_info, result, all_info);
                }
                else
                {
                    flag = CL.Judge_Attrs_Exist(sql_temp.get_table_name(), sql_temp.command_left_vector);
                    if (flag == false)
                    {
                        cout << "Some attributes do not exist!!" << endl;
                        return;
                    }
                    
                    /*create condition vector*/
                    for (int i = 0; i < sql_temp.command_op_vector.size(); i++)
                    {
                        string name = sql_temp.command_left_vector[i];
                        string op = sql_temp.command_op_vector[i];
                        int id;
                        
                        if (op == "=") id = 0;
                        else if (op == "<>") id = 1;
                        else if (op == "<") id = 2;
                        else if (op == ">") id = 3;
                        else if (op == "<=") id = 4;
                        else if (op == ">=") id = 5;
                        
                        string value = sql_temp.command_right_vector[i];
                        Condition con_d(name, value, id);
                        
                        if (CL.Judge_Attr_Index_Exist(sql_temp.get_table_name(), name))
                        {
                            con_special.push_back(con_d);
                            attr_special.push_back(CL.Get_Attr_Info_Record(sql_temp.get_table_name(), name));
                        }
                        else
                        {
                            con_normal.push_back(con_d);
                            attr_normal.push_back(CL.Get_Attr_Info_Record(sql_temp.get_table_name(), name));
                        }
                    }
                    
                    all_info = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                    if (attr_special.size() == 0) //no index condition
                    {
                        record_num = RM.Record_SelectValue(sql_temp.get_table_name(), all_info, result, all_info, con_normal);
                    }
                    else
                    {
                        IM.searchMutiCondition(CL.Get_Used_Database(), sql_temp.get_table_name(), con_special, Result);
                        record_num = RM.Record_SelectValue(sql_temp.get_table_name(), Result, all_info, result, all_info, con_normal);
                    }
                }
                
                string info = "|";
                for (int i = 0; i < all_info.size(); i++)
                {
                    info = all_info[i].attr_name;
                    cout << setw(10) << info;
                }
                cout<<endl;
                /*print result*/
                for (int i = 0; i < result.size(); i++)
                {
                    for (int j = 0; j < result[i].size(); j++)
                    {
                        cout << setw(10) << result[i][j];
                    }
                    cout<<endl;
                }
                
                info = to_string(record_num);
                info += " rows in set.";
                cout << info << endl;
                break;
            }
                
            case (SQL_CREATE_TABLE):
            {
                CL.Create_Table(sql_temp);
                //cout << "Query OK, 0 rows affected" << endl;
                break;
            }
                
            case (SQL_USE_DATABASE):
            {
                CL.Use_Database(sql_temp.get_database_name());
                break;
            }
                
            case (SQL_SHOW_DATABASES):
            {
                vector<string> Rem = CL.Get_All_Databases();
                cout << "+---------------------+" << endl;
                cout << "|       Database      |" << endl;
                cout << "+---------------------+" << endl;
                for (int i = 0; i < Rem.size(); i++)
                {
                    string temp = "|" + Rem[i];
                    cout << temp << endl;
                }
                cout << "+---------------------+" << endl;
                break;
            }
                
            case (SQL_SHOW_TABLES):
            {
                vector<string> Rem = CL.Get_All_Table_Name();
                cout << "+---------------------+" << endl;
                cout << "|       Tables        |" << endl;
                cout << "+---------------------+" << endl;
                for (int i = 0; i < Rem.size(); i++)
                {
                    string temp = "|" + Rem[i];
                    cout << temp << endl;
                }
                cout << "+---------------------+" << endl;
                break;
            }
                
            case (SQL_CREATE_DATABASE):
            {
                CL.Create_Database(sql_temp.get_database_name());
                break;
            }
                
                /*update table set id = 5 where ... and ...*/
            case (SQL_UPDATE):
            {
                vector<int> Result;
                vector<class Attribute> all_info;
                vector<string> value;
                vector<class Attribute> select_attr;
                int record_num;
                
                bool flag = true;
                flag = CL.Judge_Table_Exist(sql_temp.get_table_name());
                
                if (flag == false)
                {
                    cout << "This table does not exists!!" << endl;
                    return;
                }
                
                flag = CL.Judge_Attr(sql_temp.get_col_name(), sql_temp.get_table_name());
                if (flag == false)
                {
                    cout << sql_temp.get_col_name() + " does not exists!!" << endl;
                    return;
                }
                
                flag = CL.Judge_Attrs_Exist(sql_temp.get_table_name(), sql_temp.command_left_vector);
                if (flag == false)
                {
                    cout << "Some attributes do not exist!!" << endl;
                    return;
                }
                
                /*create condition vector*/
                for (int i = 0; i < sql_temp.command_op_vector.size(); i++)
                {
                    string name = sql_temp.command_left_vector[i];
                    string op = sql_temp.command_op_vector[i];
                    int id;
                    
                    if (op == "=") id = 0;
                    else if (op == "<>") id = 1;
                    else if (op == "<") id = 2;
                    else if (op == ">") id = 3;
                    else if (op == "<=") id = 4;
                    else if (op == ">=") id = 5;
                    
                    string value = sql_temp.command_right_vector[i];
                    Condition con_d(name, value, id);
                    
                    if (CL.Judge_Attr_Index_Exist(sql_temp.get_table_name(), name))
                    {
                        con_special.push_back(con_d);
                        attr_special.push_back(CL.Get_Attr_Info_Record(sql_temp.get_table_name(), name));
                    }
                    else
                    {
                        con_normal.push_back(con_d);
                        attr_normal.push_back(CL.Get_Attr_Info_Record(sql_temp.get_table_name(), name));
                    }
                }
                
                all_info = CL.Get_Attr_Info_All_Record(sql_temp.get_table_name());
                value.push_back(sql_temp.get_col_value());
                select_attr.push_back(CL.Get_Attr_Info_Record(sql_temp.get_table_name(), sql_temp.get_col_name()));
                if (con_special.size() == 0) //no index condition
                {
                    record_num = RM.Record_Update(sql_temp.get_table_name(), select_attr, value, all_info, con_normal);
                }
                else
                {
                    IM.searchMutiCondition(CL.Get_Used_Database(), sql_temp.get_table_name(), con_special, Result);
                    record_num = RM.Record_Update(sql_temp.get_table_name(), Result, select_attr, value, all_info, con_normal);
                }
                if(record_num == -1)
                    cout<<"Duplicate entry for primary key or unique key"<<endl;
                else
                    cout << "Query OK, " + to_string(record_num) + " rows affected"<<endl;
                break;
            }
                
            default:
                break;
        }
    }
    catch (CatalogError e)
    {
        throw e;
    }
}

void API::sql_execfile(SqlCommand sql_temp)
{
    ifstream read_in(sql_temp.get_file_name());
    string sql_com;
    
    if (!read_in) //判断文件是否打开
    {
        cout << "Can not open file!" << endl;
        return;
    }
    
    while (getline(read_in, sql_com)) //sql_com是已经存放在里面的sql命令
    {
        InterPreter inter_temp;
        
        sql_com = inter_temp.initial_sentense(sql_com);
        
        if (sql_com == "error") //如果出错的话，打印出来就吼了
            cout << "---- Syntax Error! ---- \n" << endl;
        else
        {
            SqlCommand c = inter_temp.Final_expression(sql_com);
            API api(c);
            api.execute(c); //是一个正常的语句的话，重新执行一遍整个语句
        }
    }
}
