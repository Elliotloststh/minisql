#include "IndexManager.h"
#include "BufferManager.h"

IndexManager::IndexManager(){
}

IndexManager::~IndexManager(){
  for (auto e = intIndexMap.begin(); e != intIndexMap.end();e++){
    if(e->second){
      e->second->save();
      delete e->second;
    }
  }
  for (auto e = floatIndexMap.begin(); e != floatIndexMap.end();e++){
    if(e->second){
      e->second->save();
      delete e->second;
    }
  }
  for (auto e = charIndexMap.begin(); e != charIndexMap.end();e++){
    if(e->second){
      e->second->save();
      delete e->second;
    }
  }
}

void IndexManager::updateOne(string path,string key,int type,int offset){
  deleteOneBykey(path, key, type);
  insertOne(path, key, type, offset);
}

int IndexManager::getDegree(int blocksize,int type){
  int totalsize = getKeySize(type)+sizeof(int);
  return (blocksize-4) / totalsize ;
}

void IndexManager::createIndex(string database,string table,string attribute,string index,int type){
  string path = database + "+" + table + "+" + attribute + "+" + index;
  createIndexBypath(path, type,index);
}

void IndexManager::dropIndex(string database,string table,string attribute,string index,int type){
  string path = database + "+" + table + "+" + attribute + "+" + index;
  dropIndexBypath(path, type,index);
}

void IndexManager::createIndexBypath(string path,int type,string indexname){
  
  int key_size = getKeySize(type);
  int blocksize=Buffer::BufferBlock::BLOCK_BYTE_SIZE;
  int tree_degree = getDegree(blocksize, type);
  
  if(bm.getBlock(indexname,0)==NULL)
    bm.newFile(indexname);
  if(type==TYPE_INT){
    BPlusTree<int> *treetmp = new BPlusTree<int>(indexname, tree_degree, key_size);
    intIndexMap[path] = treetmp;
  }
  else if(type==TYPE_FLOAT){
    BPlusTree<float> *treetmp = new BPlusTree<float>(indexname, tree_degree, key_size);
    floatIndexMap[path] = treetmp;
  }
  else if(type>=0 && type <=255){
    BPlusTree<string> *treetmp = new BPlusTree<string>(indexname, tree_degree, key_size);
    charIndexMap[path] = treetmp;
  }
  else {
    cout << "createIndex() Error:invalid type" << endl;
    exit(0);
  }
  
}

void IndexManager::dropIndexBypath(string path,int type,string indexname){
  int key_size = getKeySize(type);
  int blocksize;
  int tree_degree = getDegree(blocksize, type);
  if(type==TYPE_INT){
    BPlusTree<int> *treetmp = intIndexMap[path];
    delete treetmp;
    treetmp = NULL;
    intIndexMap.erase(path);
  }
  else if(type==TYPE_FLOAT){
    BPlusTree<float> *treetmp = floatIndexMap[path];
    delete treetmp;
    treetmp = NULL;
    floatIndexMap.erase(path);
  }
  else if(type>=0 && type <=255){
    BPlusTree<string> *treetmp = charIndexMap[path];
    delete treetmp;
    treetmp = NULL;
    charIndexMap.erase(path);
  }
  else {
    cout << "createIndex() Error:invalid type" << endl;
    exit(0);
  }
  bm.deleteFile(indexname);
}

void IndexManager::loadIndexFile(string path,int type){
  string restr("(\\w+)\\+(\\w+)\\+(\\w+)\\+(\\w+-\\w+-index)");
  regex pattern(restr);
  smatch match_result;
  string indexname;
  if (regex_match(path, match_result,pattern))
    indexname = match_result[4];
  else{
    cout << "loadIndexFile() Error: Invalid path" << endl;
    exit(1);
  }
  createIndexBypath(path, type, indexname);
}

int IndexManager::getKeySize(int keytype){
  if(keytype==-1)
    return sizeof(float);
  else if(keytype ==0)
    return sizeof(int);
  else if(keytype >0 && keytype <=255)
    return keytype;
  else{
    cout << "getKeySize() Error: Unknown keytype" << endl;
    exit(1);
  }
}

void IndexManager::keyConvert(string keyvalue,int type){
    stringstream convert;
    convert << keyvalue;
    if (type == TYPE_INT)
        convert >> keyint;
    else if(type==TYPE_FLOAT)
        convert >> keyfloat;
    else if(type>0 && type <=255)
        keycharn = convert.str();
    else{
        cout << "keyConvert() Error: Invalid type" << endl;
        exit(1);
    }
}

int IndexManager::searchOne(string path,string key,int type){
  int offset;
  keyConvert(key, type);
  if (type == TYPE_INT)
  {
    auto e = intIndexMap.find(path);
    if(e==intIndexMap.end()){
      loadIndexFile(path, type);
      e = intIndexMap.find(path);
    }
    offset = e->second->searchOne(keyint);
  }
  else if(type ==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e==floatIndexMap.end()){
      loadIndexFile(path, type);
      e = floatIndexMap.find(path);
    }
    offset = e->second->searchOne(keyfloat);
  }
  else if(type>0 && type<=255)
  {
    auto e = charIndexMap.find(path);
    if(e==charIndexMap.end()){
      loadIndexFile(path, type);
      e = charIndexMap.find(path);
    }
    offset = e->second->searchOne(keycharn);
  }
  else{
    cout << "searchOne() Error: invalid type" << endl;
    exit(1);
  }
  return offset;
}

string IndexManager::getPath(string database,string table,string attribute){
  string index = table + "-" + attribute + "-" + "index";
  string path = database + "+" + table + "+" + attribute + "+" + index;
  return path;
}

int IndexManager::getAttributeType(string table,string attribute){
  vector<string> get = CL.Get_Attr_Info(table, attribute);
  int type = atoi(get[1].c_str());
  return type;
}

void IndexManager::searchCondition(string database,string table,Condition& condition,set<int>& result){
  string path = getPath(database, table, condition.attributeName);
  int type = getAttributeType(table, condition.attributeName);
  keyConvert(condition.value, type);
  if(type==TYPE_INT){
    auto e = intIndexMap.find(path);
    if(e==intIndexMap.end()){
      loadIndexFile(path, type);
      e = intIndexMap.find(path);
    }
    e->second->searchMuticondition(keyint, condition.operate, result);
  }
  else if(type==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e==floatIndexMap.end()){
      loadIndexFile(path, type);
      e = floatIndexMap.find(path);
    }
    e->second->searchMuticondition(keyfloat, condition.operate, result);
  }
  else if(type>=0 && type <=255){
    auto e = charIndexMap.find(path);
    if(e==charIndexMap.end()){
      loadIndexFile(path, type);
      e = charIndexMap.find(path);
    }
    e->second->searchMuticondition(keycharn, condition.operate, result);
  }
  else{
    cout << "searchCondition() Error:Invalid type" << endl;
    exit(1);
  }
}

void IndexManager::searchMutiCondition(string database,string table,vector<Condition>& condition,vector<int>& result){
    vector< set<int> > resultorg;
    resultorg.resize(condition.size());
    int maxsize = 0;
    int minsize = 10000000;
    for (int i = 0; i < condition.size(); i++)
    {
        searchCondition(database, table, condition[i], resultorg[i]);
        if(resultorg[i].size()>maxsize)
            maxsize = resultorg[i].size();
        if(resultorg[i].size()<minsize)
            minsize = resultorg[i].size();
    }
    
    if(resultorg.size()==1){
        set<int> s =resultorg[0];
        for(auto e=s.begin();e!=s.end();e++)
            result.push_back(*e);
    }
    else{
        result.resize(maxsize);
        auto e =result.begin();
        for (int i = 0; i < resultorg.size() - 1; i++)
            e=set_intersection(resultorg[i].begin(), resultorg[i].end(), resultorg[i + 1].begin(), resultorg[i + 1].end(), result.begin());
        result.resize(e-result.begin());
    }
    //    result.resize(minsize);
}

void IndexManager::searchMany(string path, string keyfloor, string keyceil, int type, vector<int> &result){
  keyConvert(keyfloor, type);
  if (type == TYPE_INT)
  {
    auto e = intIndexMap.find(path);
    if(e==intIndexMap.end()){
      cout << "searchOne() Error: invalid filepath" << endl;
      exit(1);
    }
    int keyfloortmp = keyint;
    keyConvert(keyceil, type);
    int keyceiltmp = keyint;
    e->second->searchIntervalEx(keyfloortmp, keyceiltmp, result);
  }
  else if(type ==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e==floatIndexMap.end()){
      cout << "searchOne() Error: invalid filepath" << endl;
      exit(1);
    }
    float keyfloortmp = keyfloat;
    keyConvert(keyceil, type);
    float keyceiltmp = keyfloat;
    e->second->searchIntervalEx(keyfloortmp, keyceiltmp, result);
  }
  else if(type>0 && type<=255)
  {
    auto e = charIndexMap.find(path);
    if(e==charIndexMap.end()){
      cout << "searchOne() Error: invalid filepath" << endl;
      exit(1);
    }
    string keyfloortmp = keycharn;
    keyConvert(keyceil, type);
    string keyceiltmp = keycharn;
    e->second->searchIntervalEx(keyfloortmp, keyceiltmp, result);
  }
  else{
    cout << "searchOne() Error: invalid type" << endl;
    exit(1);
  }
}

void IndexManager::insertOne(string path,string key,int type,int offset){
  keyConvert(key, type);
  if(type==TYPE_INT){
    auto e = intIndexMap.find(path);
    if(e ==intIndexMap.end()){
      loadIndexFile(path, type);
      e = intIndexMap.find(path);
    }
    e->second->insertKey(keyint,offset);
  }
  else if(type ==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e ==floatIndexMap.end()){
      loadIndexFile(path, type);
      e= floatIndexMap.find(path);
    }
    e->second->insertKey(keyfloat,offset);
  }
  else if(type >0 && type<=255){
    auto e = charIndexMap.find(path);
    if(e ==charIndexMap.end()){
      loadIndexFile(path, type);
      e = charIndexMap.find(path);
    }
    e->second->insertKey(keycharn,offset);
  }
  else{
    cout << "insertOne Error():invalid type" << endl;
    exit(1);
  }
}

void IndexManager::deleteOneBykey(string path,string key,int type){
  keyConvert(key, type);
  if(type==TYPE_INT){
    auto e = intIndexMap.find(path);
    if(e==intIndexMap.end()){
      loadIndexFile(path, type);
      e = intIndexMap.find(path);
    }
    e->second->DeleteKey(keyint);
  }
  else if(type==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e==floatIndexMap.end()){
      loadIndexFile(path,type);
      e = floatIndexMap.find(path);
    }
    e->second->DeleteKey(keyfloat);
  }
  else if(type>0 && type<=255){
    auto e = charIndexMap.find(path);
    if(e==charIndexMap.end()){
      loadIndexFile(path, type);
      e = charIndexMap.find(path);
    }
    e->second->DeleteKey(keycharn);
  }
  else{
    cout << "deleteOne() Error:invalid type" << endl;
    exit(1);
  }
}

void IndexManager::deleteOneByoffset(string path,string key,int type,int offset){
  keyConvert(key, type);
  if(type==TYPE_INT){
    auto e = intIndexMap.find(path);
    if(e==intIndexMap.end()){
      cout << "deleteOne() Error:invalid filepath" << endl;
      exit(1);
    }
    e->second->DeleteKeyByoffset(keyint,offset);
  }
  else if(type==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e==floatIndexMap.end()){
      cout << "deleteOne() Error:invalid filepath" << endl;
      exit(1);
    }
    e->second->DeleteKeyByoffset(keyfloat,offset);
  }
  else if(type>0 && type<=255){
    auto e = charIndexMap.find(path);
    if(e==charIndexMap.end()){
      cout << "deleteOne() Error:invalid filepath" << endl;
      exit(1);
    }
    e->second->DeleteKeyByoffset(keycharn,offset);
  }
  else{
    cout << "deleteOne() Error:invalid type" << endl;
    exit(1);
  }
}

void IndexManager::deleteAll(string database,string table,string attribute,string index,int type){
  dropIndex(database,table,attribute,index,type);
  createIndex(database,table,attribute,index,type);
}

bool IndexManager::keyRepeat(string path,string key,int type){
  keyConvert(key, type);
  if(type==TYPE_INT){
    auto e = intIndexMap.find(path);
    if(e ==intIndexMap.end()){
      loadIndexFile(path, type);
      e = intIndexMap.find(path);
    }
    return e->second->checkKeyRepeat(keyint);
  }
  else if(type ==TYPE_FLOAT){
    auto e = floatIndexMap.find(path);
    if(e ==floatIndexMap.end()){
      loadIndexFile(path,type);
      e = floatIndexMap.find(path);
    }
    return e->second->checkKeyRepeat(keyfloat);
  }
  else if(type >0 && type<=255){
    auto e = charIndexMap.find(path);
    if(e ==charIndexMap.end()){
      loadIndexFile(path, type);
      e = charIndexMap.find(path);
    }
    return e->second->checkKeyRepeat(keycharn);
  }
  else{
    cout << "insertOne Error():invalid type" << endl;
    exit(1);
  }
}

