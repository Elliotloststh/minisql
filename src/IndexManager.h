#ifndef __IndexManager_h_
#define __IndexManager_h_

#include "BPlusTree.h"
#include "RecordManager.hpp"
#include "CatalogManager.h"
#include<iostream>
#include<sstream>
#include<cstdlib>
#include<map>
#include<vector>
#include<set>
#include<algorithm>
#include<regex>

using namespace std;
using namespace BufferManager;
extern CatalogManager CL;
extern Buffer bm;

struct index_info
{
    string index_file_name;
    char key_type;     // type_of_value 0--int -1--float n--char(n) n=1~255
    string key_value;  // the value
    int offset;    // the record offset in the table file
};

class IndexManager
{
    
    typedef map<string, BPlusTree<int>*> intMap;
    typedef map<string, BPlusTree<float> *> floatMap;
    typedef map<string, BPlusTree<string> *> charMap;
    static const int TYPE_INT = 0;
    static const int TYPE_FLOAT = -1;
    
private:
    intMap intIndexMap;
    floatMap floatIndexMap;
    charMap charIndexMap;
    int keyint;
    float keyfloat;
    string keycharn;
    
    
    int getKeySize(int keytype);
    void keyConvert(string keyvalue,int type);
    int getDegree(int blocksize, int type);
    void createIndexBypath(string path, int type, string indexname);
    void dropIndexBypath(string path, int type,string indexname);
    void searchCondition(string database,string table,Condition &condition, set<int> &result);
    string getPath(string database, string table, string attribute);
    int getAttributeType(string table, string attribute);
    void loadIndexFile(string path,int type);
    
public:
    // 所有的path是建立出来的索引文件名 "dbname_tablename_columnname"
    // type是主键的类型 有int  float char(n) 三种
    // 一条记录在索引文件中的存储就是一个键值key和这条记录在table文件中的编号
    
    //检查key是否重复 key值用string存储,type为-1～255
    bool keyRepeat(string path, string key, int type);
    
    int searchOne(string path, string key, int type);
    void searchMany(string path, string keyfloor,string keyceil, int type,vector<int> &resule);
    void insertOne(string path, string key, int type, int offset);
    void deleteOneBykey(string path, string key,int type);
    void updateOne(string path, string key, int type, int offset);
    void deleteOneByoffset(string path, string key, int type, int offset);
    void deleteAll(string database,string table,string attribute,string index,int type);
    void searchMutiCondition(string database,string table,vector<Condition> &condition ,vector<int>& result);
    
    IndexManager();
    ~IndexManager();
    void createIndex(string database, string table, string attribute, string index,int type);
    void dropIndex(string database, string table ,string attribute,string index, int type);
};

#endif
