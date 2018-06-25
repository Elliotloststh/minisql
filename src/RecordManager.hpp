#ifndef RecordManager_hpp
#define RecordManager_hpp

#include <string>
#include <vector>
#include "BufferManager.h"

#define BLOCKSIZE 4096
#define INT 0
#define FLOAT 1
#define CHAR 2

#define PRIMARY 0
#define UNIQUE 1
#define EMPTY 2

using namespace std;
using BYTE = char;
using namespace BufferManager;
using Block = Buffer::BufferBlock;
typedef vector<vector<string>> SelectResult;//select的结果，如<<1,1.2,ad>,<2,1.2,ds>>

class Attribute
{
public:
    string attr_name;
    int attr_type;	//属性的数据类型，分别为INT, FLOAT, CHAR
    int attr_key_type;//属性的主键类型，分别为PRIMARY, UNIQUE, NULL(EMPTY)
    int attr_len; 	//属性所存放的数据的长度
    int attr_id;    //属性在表中是第几个
};

class Condition     //delete或者select的条件
{
    
public:
    const static int OPERATOR_EQUAL = 0; // "="
    const static int OPERATOR_NOT_EQUAL = 1; // "<>"
    const static int OPERATOR_LESS = 2; // "<"
    const static int OPERATOR_MORE = 3; // ">"
    const static int OPERATOR_LESS_EQUAL = 4; // "<="
    const static int OPERATOR_MORE_EQUAL = 5; // ">="
    
    Condition(string a,string v,int o);
    
    string attributeName;
    string value;           // the value to be compared
    int operate;            // the type to be compared
    
    bool ifRight(int content);
    bool ifRight(float content);
    bool ifRight(string content);   //多个重载，用于判断条件是否满足
};


class RecordManager
{
public:
    RecordManager(){};
    ~RecordManager(){};
    
    //为unique的索引插入数据
    void Record_CreateIndex(string table_name, string attribute_name, string index_name, vector<Attribute>& Attribute_vec);
    //插入一条记录，参数表名，value的vector（都用string传入），Attribute的vector。返回插入数据在块中的地址
    int Record_InsertValue(const string table_name, vector<string>& value_vec, vector<Attribute>& Attribute_vec);
    //删除表的所有记录，返回删除的记录条数
    int Record_DeleteAllValue(const string table_name, vector<Attribute>& Attribute_vec);
    //根据condition的vector删除记录，vector中的条件均以and连接，如果要处理or，将or两边的条件都delete一次即可，返回删除的记录条数
    int Record_DeleteValue(const string table_name, vector<Condition>& Condition_vec, vector<Attribute>& Attribute_vec);
    //不带条件select某几个属性，外部传入result，函数将符合要求的记录push进result，返回查找的记录条数
    int Record_SelectAll(const string table_name, vector<Attribute>& select_attr_vec,
    	SelectResult& result, vector<Attribute>& Attribute_vec);
    
    //带条件的select，增加一个condition 的vector，返回查找的记录条数
    int Record_SelectValue(const string table_name, vector<Attribute>& select_attr_vec,
    	SelectResult& result, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec);
    //重载，在index给的偏移量中查找，返回查找的记录条数
    int Record_SelectValue(const string table_name, vector<int>& offset, vector<Attribute>& select_attr_vec,SelectResult& result, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec);
    
    //update语句:update tablename select_attr = value where condition
    int Record_Update(const string table_name, vector<Attribute>& select_attr_vec, vector<string>& value_vec, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec);
    //重载，在index给的偏移量中更新
    int Record_Update(const string table_name, vector<int>& offset, vector<Attribute>& select_attr_vec, vector<string>& value_vec, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec);
private:
    int get_Record_Size(string table_name, vector<Attribute>& Attribute_vec);
    bool able_to_insert(string table_name, vector<Attribute>& Attribute_vec, vector<string>& value_vec);
    bool able_to_update(string table_name, vector<Attribute>& select_attr_vec, vector<string>& value_vec, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec);
    bool Repeat(string table_name, vector<Attribute>& select_attr_vec, vector<Attribute>& Attribute_vec);
    void insert_index(string table_name, vector<Attribute>& Attribute_vec, vector<string>& value_vec);
    int get_TotalLength(vector<Attribute>& Attribute_vec);
    bool meet_tail(int query_addr, Block* chunk, string table_name);
    void update_index_tail(string table_name, vector<Attribute>& Attribute_vec, int count1, BYTE* buffer);
    void delete_index(string table_name, vector<Attribute>& Attribute_vec, BYTE* buffer);
    void update_index(string table_name,vector<Attribute>& Attribute_vec,Attribute select_attr,string value, BYTE* buffer);
};


#endif /* RecordManager_hpp */
