#ifndef CM
#define CM

#include <iostream>
#include <string>
#include <vector>

#include "sqlcommand.hpp"
#include "RecordManager.hpp"

#define MAX_CHAR_SIZE 255
#define MAX_NAME_LENGTH 32


class CatalogManager{
public:
    CatalogManager();
    ~CatalogManager();
    
//********************数据库相关********************//
    
    //创建数据库，新建名为DB_Name的文件夹，里面创建数据字典data_dic
    //data_dic格式:
    //表数量\n
    //{ 表名 属性数 实际记录长度 表的block数 {属性名、数据类型(int)、数据长度、属性类型、索引名、索引的block数}* }* \n
    //属性类型为primary、unique、null   索引名null、primary_index、自定义
    //数据类型：0为int，>0为char最多255，-1为float
    //属性数最多32，属性名最多32个字符
    void Create_Database(string DB_Name);
    
    //删除数据库
    void Drop_Database(string DB_Name);
    
    //使用数据库
    void Use_Database(string DB_Name);
    
    
//--------------------数据库相关--------------------//
    
    
//********************表格相关********************//
    
    //创建表，并且更改data_dic中的内容，表类型如上
    void Create_Table(SqlCommand& cmd);
    
    //删除表并删除索引
    void Drop_Table(string Table_Name);
    
    //创建索引，为指定属性
    //在已经建表的基础上
    void Create_Index(string Index_Name, string Attr_Name, string DB_Name);
    
    //删除索引
    void Drop_Index(string Index_Name);
    
    //删除表内所有元组
    void Empty_Table(string Table_Name);
    
    //增加元组
    void Add_Tuple(int num, string Table_Name);
    
    //减少元组
    void Delete_Tuple(int num, string Table_Name);
    
//--------------------表格相关--------------------//


//********************获取信息相关********************//
    
    //获得当前使用的数据库名
    string Get_Used_Database();
    
    //获得所有数据库的名字
    vector<string> Get_All_Databases();
    
    //获得当前所有表格名
    vector<string> Get_All_Table_Name();
    
    //获得已经建立表的个数
    int Get_All_Tables_Num();
    
    //获取所有属性的信息
    vector<string> Get_Attr_Info_All(string Table_Name);
    
    //获取所有属性的信息
    vector<Attribute> Get_Attr_Info_All_Record(string Table_Name);
    
    //获取指定属性的信息
    Attribute Get_Attr_Info_Record(string Table_Name, string Attr);
    
    //获得表的属性数
    int Get_Table_Attr_Num(string Table_Name);
    
    //获取指定属性的信息
    vector<string> Get_Attr_Info(string Table_Name, string Attr);
    
    //获取数据库中所有的索引信息
    //{表格名，属性名，索引名}
    vector<string> Get_All_Index();
    
    //{属性名，索引名}*
    vector<string> Get_Table_Index(string Table_Name);
    
    //获取xx表xx属性的索引信息
    string Get_Index(string Table_Name, string Attr);
    
    //获得表的实际记录长度
    int Get_Table_Record_Num(string Table_Name);
    
    //获得表中属性的id
    int Get_Attr_Id(string Table_Name, string Attr_Name);
    
    //获得block数
    int Get_Block_Num(string name);
    
    //设置block数
    void Set_Block_Num(string name, int block_num);
    
    //block数++
    void Increase_Block_Num(string name);
    
    //获得表的block数
    int Get_Table_Block_Num(string Table_Name);
    
    //获得index的block数
    int Get_Index_Block_Num(string Index_Name);
    
    //设置表的block数
    void Set_Table_Block_Num(string Table_Name, int block_num);
    
    //设置index的block数
    void Set_Index_Block_Num(string Index_Name, int block_num);
    
    
//--------------------获取信息相关--------------------//
    
    
//********************验证相关********************//

    // 验证表格是否存在
    bool Judge_Table_Exist(string Table_Name);

    //验证xx属性是否可以建立索引
    bool Judge_Index(string attr_name, string Table_Name);
    
    // 验证xx属性组在xx表中是否存在
    bool Judge_Attrs_Exist(string Table_Name, vector<string> &Attrs);
    
    //判断是否为表的属性名
    bool Judge_Attr(string attr_name, string Table_Name);
    
    //验证插入记录是否满足表格属性的类型定义
    bool Judge_Data(vector<string> &Datas, string Table_Name);
    
    bool Judge_Attr_Index_Exist(string Table_Name, string attr_name);
//--------------------验证相关--------------------//
    
    string used_Database; // 被选用的database
    
private:
    
//********************验证相关********************//
    bool Judge_Database_Redundancy(string database);// 验证数据库存在
    
    bool Judge_Attrs(vector<string> &infos);//建表验证所有属性信息
//********************验证相关********************//
    
    vector<string> split(string s); // 分裂字符串
    bool Judge_File_Exist(string fileName, string path); // 判断文件是否存在
    vector<string> Get_All_Lines(string path); // 获取文件所有内容
    void Write_Into_File(string path, vector<string> contents); // 写
    string itoa(int x);// int变字符串
    bool Judge_String_Length(string &s);
};


class table
{
public:
    table()
    {
        attr_num = 0;
        size = 0;
    }
    string table_name; //表格名称
    int attr_num; //属性数量
    int size; //实际记录的长度
    int table_block_size; //表的block数
    vector<string> attr_name; //属性名称
    vector<int> attr_type; //属性类型 float -1  int 0 char 0-255
    vector<int> attr_length; //属性长度
    vector<string> key_constraint; //primary unique null属性的限制
    vector<string> index; //null 属性对应的索引名
    vector<int> index_block_size;//索引的block数
};

#endif
