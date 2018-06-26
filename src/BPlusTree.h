//
//  bplustree.h
//  test
//
//  Created by 李家纬 on 2018/6/16.
//  Copyright © 2018年 李家纬. All rights reserved.
//

#ifndef bplustree_h
#define bplustree_h

#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <vector>
#include <queue>
#include <set>
#include "BufferManager.h"

using namespace std;
using namespace BufferManager;
extern Buffer bm;

template <class Key>
class BPlusTree;
template <class Key>
class TreeNode
{
    friend class BPlusTree<Key>;
    
private:
    bool is_leaf;    // 1 leaf 0 nonleaf
    int total_child; // in leaf: total keys <=n-1 // in nonleaf:total childs <=n
    int degree;      // degree == n
    int block_id;
    TreeNode *next; // for leaf travel
    TreeNode *parent;
    vector<Key> keys;
    vector<TreeNode *> childs;
    vector<int> record_offsets;
    
public:
    TreeNode(int degree_, bool is_leaf_ = true);
    ~TreeNode(){};
    bool checkMinimum();
    void DeleteRecord(int offset);
    int keyBinarySearch(Key &target);
    int recordEasySearch(Key &target, int offset);
    int childEasySearch(TreeNode *target);
    TreeNode *split();
    
    void debugCheckDegree();
};

template <class Key>
TreeNode<Key>::TreeNode(int degree_, bool is_leaf_) : is_leaf(is_leaf_), total_child(0), degree(degree_), block_id(0), next(NULL), parent(NULL)
{
}

template <class Key>
int TreeNode<Key>::keyBinarySearch(Key &target)
{
    if (total_child == 0)
    {
        cout << "keyBinarySearch() Error:TreeNode No keys" << endl;
        exit(1);
        //  throw(string("keyBinarySearch() Error: No element"));
    }
    
    auto position = lower_bound(keys.begin(), keys.end(), target);
    return position - keys.begin();
}

template <class Key>
int TreeNode<Key>::childEasySearch(TreeNode *target)
{ // not ordered search can't use binary search
    if (total_child == 0)
    {
        cout << "childBinarySearch() Error:TreeNode No childs" << endl;
        exit(1);
        // throw(string("childBinarySearch() Error: No element"));
    }
    for (auto e = childs.begin(); e != childs.end(); e++)
        if (*e == target)
            return e - childs.begin();
    cout << "childEasySearch() Error: Not found target" << endl;
    exit(1);
    // throw(string("childEasySearch() Error: Not found target"));
}

template <class Key>
int TreeNode<Key>::recordEasySearch(Key &target, int offset)
{
    if (total_child == 0)
    {
        cout << "recordEasySearch() Error: TreeNode no records" << endl;
        exit(1);
        // throw(string("recordEasySearch() Error:No element"));
    }
    int i;
    for (i = 0; i < total_child; i++)
    {
        if (keys[i] == target && record_offsets[i] == offset)
            return i;
    }
    return -1;
}

template <class Key>
void TreeNode<Key>::debugCheckDegree()
{
    bool check;
    if (is_leaf)
        check = (total_child <= degree - 1) && (keys.size() == total_child) && (record_offsets.size() == total_child);
    else
        check = (total_child <= degree) && (keys.size() == total_child - 1) && (childs.size() == total_child);
    if (!check)
    {
        cout << "NodeCheckDegree() Error" << endl;
        exit(1);
        // throw(string("NodeCheckDegree() Error"));
    }
}

template <class Key>
TreeNode<Key> *TreeNode<Key>::split()
{ // new node born in the right
    if (this->is_leaf)
    {
        TreeNode *new_node = new TreeNode(degree, true);
        int split = (degree + 1) / 2;
        new_node->next = this->next;
        new_node->parent = this->parent; //maybe NULL
        new_node->total_child = total_child - split;
        new_node->keys.assign(keys.begin() + split, keys.end());
        new_node->record_offsets.assign(record_offsets.begin() + split, record_offsets.end());
        this->next = new_node;
        this->total_child = split;
        keys.assign(keys.begin(), keys.begin() + split);
        record_offsets.assign(record_offsets.begin(), record_offsets.begin() + split);
        return new_node;
    }
    else
    {
        TreeNode *new_node = new TreeNode(degree, false);
        int split = (degree + 1 + 1) / 2;
        new_node->parent = this->parent;
        new_node->total_child = total_child - split;
        new_node->keys.assign(keys.begin() + split, keys.end());
        new_node->childs.assign(childs.begin() + split, childs.end());
        for (auto e = new_node->childs.begin(); e != new_node->childs.end(); e++)
            (*e)->parent = new_node;
        this->total_child = split;
        keys.assign(keys.begin(), keys.begin() + split - 1);
        childs.assign(childs.begin(), childs.begin() + split);
        return new_node;
    }
}

template <class Key>
bool TreeNode<Key>::checkMinimum()
{
    int minimum;
    if (this->parent == NULL)
    { //root
        if (this->is_leaf)
            minimum = 1;
        else
            minimum = 2;
    }
    else if (this->is_leaf)
        minimum = (degree - 1 + 1) / 2;
    else
        minimum = (degree + 1) / 2;
    if (this->total_child < minimum)
        return true;
    return false;
}

template <class Key>
void TreeNode<Key>::DeleteRecord(int offset)
{
    if (offset < 0 || offset >= total_child)
    {
        cout << "DeleteRecord Error(): Invalid offset" << endl;
        exit(1);
        // throw(string("DeleteRecord Error(): Invalid offset"));
    }
    keys.erase(keys.begin() + offset);
    record_offsets.erase(record_offsets.begin() + offset);
    total_child--;
}

template <class Key>
class BPlusTree
{
    using Node = TreeNode<Key>;
    using Block = Buffer::BufferBlock;
    
    
private:
    Node *root;
    Node *leaf_head;
    string filename;
    int degree;
    int key_size;
    
    Node *searchGetLeaf(Key &target);
    Node *searchGetLeafByoffset(Node *leaf, Key &target, int offset);
    void insertLeaf(Key &target, int record, Node *leaf);
    void insertNonleaf(Key &target, Node *left, Node *right);
    void getSibling(Node *leaf, int &leafoffset, Node *&sibling, int &siblingoffset);
    void adjustAfterDelete(Node *target);
    void deleteLeaf(Key &target, Node *leaf);
    void deleteLeafByoffset(Key &target, int offset, Node *leaf);
    void leafMerge(Node *left, int leftoffset, Node *right, int rightoffset);
    void NonleafMerge(Node *left, int leftoffset, Node *right, int rightoffset);
    void leafReallocate(Node *left, int leftoffset, Node *right, int rightoffset);
    void NonleafReallocate(Node *left, int leftoffset, Node *right, int rightoffset);
    Node *getLeafHead();
    
    void searchEqual(Key &target, set<int> &result);
    void searchNonequal(Key &target, set<int> &result);
    void searchLess(Key &target, set<int> &result);
    void searchMore(Key &target, set<int> &result);
    void searchLessequal(Key &target, set<int> &result);
    void searchMoreequal(Key &target, set<int> &result);
    
    void setUsingSize(Block *block, int size);
    int getUsingSize(Block *block);
    char *getContent(Block *block);
    // related to the BUFFER ,need complete
    void loadFromdisk(Block * block);
    void load();
    
    
    
public:
    BPlusTree(string filename_, int degree_, int keysize);
    ~BPlusTree(){};
    int searchOne(Key &target);
    void searchIntervalEx(Key &floor, Key &ceil, vector<int> &result);
    void searchMuticondition(Key &value,int operation,set<int> &result);
    void insertKey(Key &target, int record);
    void DeleteKey(Key &target);
    void DeleteKeyByoffset(Key &target, int offset);
    void debugPrint();
    void save();
    bool checkKeyRepeat(Key &target);
};

// From Buffer.h
template <class Key>
void BPlusTree<Key>::setUsingSize(Block *block,int size){
    char *Blockhead = block->getBufferBlockDataHandle();
    *(int *)Blockhead = size;
}

template <class Key>
int BPlusTree<Key>::getUsingSize(Block *block){
    char *Blockhead = block->getBufferBlockDataHandle();
    return *(int *)Blockhead;
}

template<class Key>
char* BPlusTree<Key>::getContent(Block *block){
    char *Blockhead = block->getBufferBlockDataHandle();
    return Blockhead + sizeof(int);
}

template <class Key>
void BPlusTree<Key>::loadFromdisk(Block *block)
{
    block->pin();
    int value_size = sizeof(int);
    char *key_loc = getContent(block);
    char *value_loc = key_loc + key_size;
    Key ktmp;
    int offsettmp;
    while (value_loc - getContent(block) < getUsingSize(block))
    {
        ktmp = *(Key *)key_loc;
        offsettmp = *(int *)value_loc;
        insertKey(ktmp, offsettmp);
        key_loc += key_size + value_size;
        value_loc += key_size + value_size;
    }
    block->unPin();
}

template <class Key>
void BPlusTree<Key>::load()
{
    Block *block = bm.getBlock(filename,0);
    while (block != NULL)
    {
        loadFromdisk(block);
        // block = file->getNextBlock(block);
        block = bm.getNextBlock(block);
    }
}

template <class Key>
void BPlusTree<Key>::save()
{
    Block *block = bm.getBlock(filename,0);
    if(block==NULL)
        block = bm.addBlock(filename);
    
    Node *p = getLeafHead();
    int value_size = sizeof(int);
    while (p != NULL)
    {
        block->pin();
        setUsingSize(block,0);
        for (int i = 0; i < p->total_child; i++)
        {
            char *ktmp = (char *)&(p->keys[i]);
            char *otmp = (char *)&(p->record_offsets[i]);
            memcpy(getContent(block) + getUsingSize(block), ktmp, key_size);
            setUsingSize(block,getUsingSize(block) + key_size);
            memcpy(getContent(block) + getUsingSize(block), otmp, value_size);
            setUsingSize(block,getUsingSize(block) + value_size);
        }
        block->unPin();
        block = bm.getNextBlock(block);
        if(block==NULL)
            block = bm.addBlock(filename);
        p = p->next;
    }
}

template <class Key>
BPlusTree<Key>::BPlusTree(string filename_, int degree_, int keysize) : filename(filename_),
degree(degree_), key_size(keysize), root(NULL), leaf_head(NULL)
{
    load();
}

template <class Key>
TreeNode<Key> *BPlusTree<Key>::searchGetLeaf(Key &target)
{
    Node *p = root;
    if (p == NULL)
    {
        cout << "searchGetLeaf() Error:root is NULL" << endl;
        exit(1);
        // throw(string("searchGetLeaf() Error: root is NULL"));
    }
    while (!p->is_leaf)
    {
        int offset;
        offset = p->keyBinarySearch(target); //nonleaf search <=n-1
        if (offset == p->keys.size())
            p = p->childs.back();
        else if (target < p->keys[offset]) //target == key[offset] or key[offset] > target
            p = p->childs[offset];
        else
            p = p->childs[offset + 1];
    }
    while (p->keys.back() < target && p->next)
    {
        if (p->next->keys.front() <= target)
            p = p->next;
        else
            break;
    }
    return p;
}

template <class Key>
TreeNode<Key> *BPlusTree<Key>::searchGetLeafByoffset(Node *leaf, Key &target, int offset)
{
    Node *p = leaf;
    while (p->next && p->next->keys[0] <= target)
    {
        int offset = p->recordEasySearch(target, offset);
        if (offset < 0)
            p = p->next;
        else
            return p;
    }
    return NULL; //not found the offset
}

template <class Key>
TreeNode<Key> *BPlusTree<Key>::getLeafHead()
{
    Node *p = root;
    if (root == NULL)
    {
        return NULL;
        // throw(string("getLeafHead() Error:tree is empty"));
    }
    while (!p->is_leaf)
    {
        p = p->childs[0];
    }
    leaf_head = p;
    return p;
}

template <class Key>
int BPlusTree<Key>::searchOne(Key &target)
{
    if (root == NULL)
        return -1;
    Node *leaf = searchGetLeaf(target);
    int offset = leaf->keyBinarySearch(target);
    if (offset > leaf->total_child-1 ) //not found the key
        return -1;
    if (leaf->keys[offset] != target) //not found the key
        return -1;
    return leaf->record_offsets[offset];
}

template <class Key>
void BPlusTree<Key>::searchEqual(Key &value,set<int>& result){
    int offset = searchOne(value);
    if(offset!=-1)
        result.insert(offset);
}

template <class Key>
void BPlusTree<Key>::searchNonequal(Key& value,set<int>& result){
    if(root==NULL)
        return;
    Node *leafhead = getLeafHead();
    Node *p = leafhead;
    while(p!=NULL){
        for (int i = 0; i < p->total_child;i++)
            if(p->keys[i]!=value)
                result.insert(p->record_offsets[i]);
        p=p->next;
    }
}

template <class Key>
void BPlusTree<Key>::searchLess(Key& value,set<int>& result){
    if(root==NULL)
        return;
    Node *leaf = searchGetLeaf(value);
    Node *leafhead = getLeafHead();
    Node *p = leafhead;
    while(p!=leaf){
        for (int i = 0; i < p->total_child;i++)
            result.insert(p->record_offsets[i]);
        p = p->next;
    }
    int offset = p->keyBinarySearch(value);
    for (int i = 0; i < offset;i++){
        result.insert(p->record_offsets[i]);
    }
}

template <class Key>
void BPlusTree<Key>::searchMore(Key& value,set<int>& result){
    if(root==NULL)
        return;
    Node *leaf = searchGetLeaf(value);
    int offset = leaf->keyBinarySearch(value);
    if(offset!=leaf->total_child && leaf->keys[offset]>value)
        result.insert(leaf->record_offsets[offset]);
    for (int i = offset+1; i < leaf->total_child; i++)
        result.insert(leaf->record_offsets[i]);
    Node *p = leaf->next;
    while (p != NULL)
    {
        for (int i = 0; i < p->total_child;i++)
            result.insert(p->record_offsets[i]);
        p = p->next;
    }
}

template <class Key>
void BPlusTree<Key>::searchLessequal(Key& value,set<int>& result){
    if(root==NULL)
        return;
    Node *leaf = searchGetLeaf(value);
    Node *leafhead = getLeafHead();
    Node *p = leafhead;
    while(p!=leaf){
        for (int i = 0; i < p->total_child;i++)
            result.insert(p->record_offsets[i]);
        p = p->next;
    }
    int offset = p->keyBinarySearch(value);
    for (int i = 0; i < offset;i++){
        result.insert(p->record_offsets[i]);
    }
    if(offset!=p->total_child && p->keys[offset]==value)
        result.insert(p->record_offsets[offset]);
}

template <class Key>
void BPlusTree<Key>::searchMoreequal(Key& value,set<int>& result){
    if(root==NULL)
        return;
    Node *leaf = searchGetLeaf(value);
    int offset = leaf->keyBinarySearch(value);
    if(offset!=leaf->total_child && leaf->keys[offset]>=value)
        result.insert(leaf->record_offsets[offset]);
    for (int i = offset+1; i < leaf->total_child; i++)
        result.insert(leaf->record_offsets[i]);
    Node *p = leaf->next;
    while (p != NULL)
    {
        for (int i = 0; i < p->total_child;i++)
            result.insert(p->record_offsets[i]);
        p = p->next;
    }
}

template <class Key>
void BPlusTree<Key>::searchMuticondition(Key &value,int operation,set<int>& result){
    const int OPERATION_EQUAL = 0;
    const int OPERATION_NONEQUAL = 1;
    const int OPERATION_LESS = 2;
    const int OPERATION_MORE = 3;
    const int OPERATION_LESSEQUAL = 4;
    const int OPERATION_MOREEQUAL = 5;
    if(operation==OPERATION_EQUAL)
        searchEqual(value, result);
    else if(operation==OPERATION_NONEQUAL)
        searchNonequal(value, result);
    else if(operation==OPERATION_LESS)
        searchLess(value, result);
    else if(operation==OPERATION_MORE)
        searchMore(value, result);
    else if(operation==OPERATION_LESSEQUAL)
        searchLessequal(value, result);
    else if(operation==OPERATION_MOREEQUAL)
        searchMoreequal(value, result);
    else{
        cout << "searchMuticondition() Error:Invalid operation" << endl;
    }
}

template <class Key>
void BPlusTree<Key>::searchIntervalEx(Key &floor, Key &ceil, vector<int> &result)
{
    Node *p = searchGetLeaf(floor);
    int offset = p->keyBinarySearch(floor);
    int i;
    for (i = offset; i < p->total_child; i++)
    {
        if (p->keys[i] > ceil)
            return;
        // cout << p->record_offsets[i] << endl;
        result.push_back(p->record_offsets[i]);
    }
    while (p->next && p->next->keys[0] <= ceil)
    {
        p = p->next;
        int i;
        for (i = 0; i < p->total_child; i++)
        {
            if (p->keys[i] > ceil)
                return;
            // cout << p->record_offsets[i] << endl;
            result.push_back(p->record_offsets[i]);
        }
    }
}

template <class Key>
void BPlusTree<Key>::insertLeaf(Key &target, int record, Node *leaf)
{
    
    if (leaf == NULL)
    {
        cout << "insertLeaf() Error: Not exist Node" << endl;
        // throw(string("insertLeaf() Error: Not exist Node"));
        return;
    }
    if (leaf->total_child == 0)
    {
        leaf->keys.push_back(target);
        leaf->record_offsets.push_back(record);
        leaf->total_child++;
        return;
    }
    int offset = leaf->keyBinarySearch(target);
    if (offset < leaf->total_child && leaf->record_offsets[offset] == record) //record already in node
    {
        cout << "keyBinarySearch() Error: Record already innode " << endl;
        // throw(string("keyBinarySearch() Error: Record already innode"));
        return;
    }
    auto kposition = leaf->keys.begin() + offset;
    auto rposition = leaf->record_offsets.begin() + offset;
    leaf->keys.insert(kposition, target);
    leaf->record_offsets.insert(rposition, record);
    leaf->total_child++;
    if (leaf->total_child <= degree - 1)
        return;
    else
    {
        int split = (degree + 1) / 2;
        Key up = leaf->keys[split];
        Node *newnode = leaf->split();
        insertNonleaf(up, leaf, newnode);
    }
}

template <class Key>
void BPlusTree<Key>::insertNonleaf(Key &target, Node *left, Node *right)
{
    Node *parent = left->parent;
    if (parent == NULL) //root split:generate new root
    {
        Node *newnode = new TreeNode<Key>(degree, false);
        newnode->total_child = 2;
        newnode->keys.push_back(target);
        newnode->childs.push_back(left);
        newnode->childs.push_back(right);
        left->parent = newnode;
        right->parent = newnode;
        root = newnode;
        return;
    }
    else
    {
        int insert = parent->childEasySearch(left);
        auto kposition = parent->keys.begin() + insert;
        auto cposition = parent->childs.begin() + insert + 1;
        parent->keys.insert(kposition, target);
        parent->childs.insert(cposition, right);
        parent->total_child++;
        if (parent->total_child <= degree)
            return;
        else
        {
            int split = (degree + 1 + 1) / 2;
            Key up = parent->keys[split - 1];
            Node *newnode = parent->split();
            insertNonleaf(up, parent, newnode);
        }
    }
}

template <class Key>
void BPlusTree<Key>::insertKey(Key &target, int record)
{
    
    if (root == NULL)
    {
        root = new TreeNode<Key>(degree);
        root->total_child++;
        root->keys.push_back(target);
        root->record_offsets.push_back(record);
        leaf_head = root;
        return;
    }
    Node *leaf = searchGetLeaf(target);
    insertLeaf(target, record, leaf);
}

template<class Key>
bool BPlusTree<Key>::checkKeyRepeat(Key & target){
    if(root ==NULL)
        return false;
    Node *leaf = searchGetLeaf(target);
    int offset = leaf->keyBinarySearch(target);
    if(offset==leaf->total_child)
        return false;
    return true;
}

template <class Key>
void BPlusTree<Key>::getSibling(Node *target, int &targetoffset, Node *&sibling, int &siblingoffset)
{
    if (target == root)
    {
        cout << "getSibling() Error: take root as target" << endl;
        exit(1);
        // throw(string("getSibling() Error: take root as target"));
    }
    Node *parent = target->parent;
    targetoffset = parent->childEasySearch(target);
    if (targetoffset == 0)
        siblingoffset = 1;
    else if (targetoffset <= parent->total_child - 1)
        siblingoffset = targetoffset - 1;
    else
    {
        cout << "getSibling() Error: Invalid offset" << endl;
        exit(1);
        // throw(string("getSibling() Error: Invalid offset"));
    }
    sibling = parent->childs[siblingoffset];
}

template <class Key>
void BPlusTree<Key>::leafMerge(Node *left, int leftoffset, Node *right, int rightoffset)
{
    left->total_child = left->total_child + right->total_child;
    left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
    left->record_offsets.insert(left->record_offsets.end(), right->record_offsets.begin(), right->record_offsets.end());
    left->next = right->next;
    Node *parent = left->parent;
    parent->keys.erase(parent->keys.begin() + leftoffset);
    parent->childs.erase(parent->childs.begin() + rightoffset);
    parent->total_child--;
    delete right;
    if (!parent->checkMinimum())
        return;
    else
        adjustAfterDelete(parent);
}

template <class Key>
void BPlusTree<Key>::NonleafMerge(Node *left, int leftoffset, Node *right, int rightoffset)
{
    Node *parent = left->parent;
    Key down = parent->keys[leftoffset];
    left->total_child = left->total_child + right->total_child;
    left->keys.insert(left->keys.end(), down);
    left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
    for (auto e = right->childs.begin(); e != right->childs.end(); e++)
        (*e)->parent = left;
    left->childs.insert(left->childs.end(), right->childs.begin(), right->childs.end());
    parent->keys.erase(parent->keys.begin() + leftoffset);
    parent->childs.erase(parent->childs.begin() + rightoffset);
    parent->total_child--;
    delete right;
    if (!parent->checkMinimum())
        return;
    else
        adjustAfterDelete(parent);
}

template <class Key>
void BPlusTree<Key>::leafReallocate(Node *left, int leftoffset, Node *right, int rightoffset)
{
    int total = left->total_child + right->total_child;
    int split = (total + 1) / 2;
    if (left->total_child < split)
    {
        int move = split - left->total_child;
        left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.begin() + move);
        left->record_offsets.insert(left->record_offsets.end(), right->record_offsets.begin(), right->record_offsets.begin() + move);
        right->keys.erase(right->keys.begin(), right->keys.begin() + move);
        right->record_offsets.erase(right->record_offsets.begin(), right->record_offsets.begin() + move);
    }
    else
    {
        int move = left->total_child - split;
        right->keys.insert(right->keys.begin(), left->keys.end() - move, left->keys.end());
        right->record_offsets.insert(right->record_offsets.begin(), left->record_offsets.end() - move, left->record_offsets.end());
        left->keys.erase(left->keys.end() - move, left->keys.end());
        left->record_offsets.erase(left->record_offsets.end() - move, left->record_offsets.end());
    }
    left->total_child = split;
    right->total_child = total - split;
    left->parent->keys[leftoffset] = right->keys[0];
}

template <class Key>
void BPlusTree<Key>::NonleafReallocate(Node *left, int leftoffset, Node *right, int rightoffset)
{
    int total = left->total_child + right->total_child;
    int split = (total + 1) / 2;
    Node *parent = left->parent;
    if (left->total_child > split)
    {
        int move = left->total_child - split;
        Key up = parent->keys[leftoffset];
        left->keys.insert(left->keys.end(), up);
        parent->keys[leftoffset] = left->keys[split - 1];
        left->keys.erase(left->keys.begin() + split - 1);
        
        right->keys.insert(right->keys.begin(), left->keys.end() - move, left->keys.end());
        right->childs.insert(right->childs.begin(), left->childs.end() - move, left->childs.end());
        left->keys.erase(left->keys.end() - move, left->keys.end());
        left->childs.erase(left->childs.end() - move, left->childs.end());
    }
    else
    {
        int move = split - left->total_child;
        Key down = parent->keys[leftoffset];
        Key up = parent->keys[move - 1];
        parent->keys[leftoffset] = up;
        right->keys.erase(right->keys.begin() + move - 1);
        right->keys.insert(right->keys.begin(), down);
        
        left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.begin() + move);
        left->childs.insert(left->childs.end(), right->childs.begin(), right->childs.begin() + move);
        right->keys.erase(right->keys.begin(), right->keys.begin() + move);
        right->childs.erase(right->childs.begin(), right->childs.begin() + move);
    }
    left->total_child = split;
    right->total_child = total - split;
}

template <class Key>
void BPlusTree<Key>::adjustAfterDelete(Node *target)
{
    if (target->is_leaf)
    {
        if (target == root)
        {
            delete root;
            root = NULL;
            return;
        }
        Node *sibling;
        int leaf_offset, sibling_offset;
        getSibling(target, leaf_offset, sibling, sibling_offset);
        if (target->total_child + sibling->total_child < degree)
        { //Merge
            if (leaf_offset < sibling_offset)
                leafMerge(target, leaf_offset, sibling, sibling_offset);
            else
                leafMerge(sibling, sibling_offset, target, leaf_offset);
        }
        else
        { //reallocate the child
            if (leaf_offset < sibling_offset)
                leafReallocate(target, leaf_offset, sibling, sibling_offset);
            else
                leafReallocate(sibling, sibling_offset, target, leaf_offset);
        }
    }
    else
    { // nonleaf node
        if (target == root)
        {
            Node *tmp = root;
            root = root->childs[0];
            root->parent = NULL;
            delete tmp;
            return;
        }
        Node *sibling;
        int targetoffset, siblingoffset;
        getSibling(target, targetoffset, sibling, siblingoffset);
        if (target->total_child + sibling->total_child <= degree)
        {
            if (targetoffset < siblingoffset)
                NonleafMerge(target, targetoffset, sibling, siblingoffset);
            else
                NonleafMerge(sibling, siblingoffset, target, targetoffset);
        }
        else
        {
            if (targetoffset < siblingoffset)
                NonleafReallocate(target, targetoffset, sibling, siblingoffset);
            else
                NonleafReallocate(sibling, siblingoffset, target, targetoffset);
        }
    }
}

template <class Key>
void BPlusTree<Key>::deleteLeaf(Key &target, Node *leaf)
{
    int offset = leaf->keyBinarySearch(target);
    if (offset == leaf->total_child || leaf->keys[offset] != target) //Not found the key
        return;
    leaf->DeleteRecord(offset);
    if (!leaf->checkMinimum())
        return;
    else
    { //find sibling
        adjustAfterDelete(leaf);
    }
}

template <class Key>
void BPlusTree<Key>::deleteLeafByoffset(Key &target, int offset, Node *leaf)
{
    int deleteoffset = leaf->recordEasySearch(target, offset);
    if (deleteoffset == -1)
        return;
    leaf->DeleteRecord(offset);
    if (!leaf->checkMinimum())
        return;
    else
    { //find sibling
        adjustAfterDelete(leaf);
    }
}

template <class Key>
void BPlusTree<Key>::DeleteKeyByoffset(Key &target, int offset)
{
    Node *p = searchGetLeaf(target);
    p = searchGetLeafByoffset(p,target, offset);
    if (p == NULL)
        return;
    deleteLeafByoffset(target, offset, p);
}

template <class Key>
void BPlusTree<Key>::DeleteKey(Key &target)
{
    Node *p = searchGetLeaf(target);
    deleteLeaf(target, p);
}

template <class Key>
void BPlusTree<Key>::debugPrint()
{
    queue<Node *> bfsq;
    if (root == NULL){
        cout << "root is NULL" << endl;
        return;
    }
    bfsq.push(root);
    while (!bfsq.empty())
    {
        Node *ntmp = bfsq.front();
        int i;
        if (ntmp->is_leaf)
        {
            for (i = 0; i < ntmp->total_child; i++)
                cout << ntmp->keys[i] << " ";
        }
        else
        {
            for (i = 0; i < ntmp->total_child - 1; i++)
            {
                cout << ntmp->keys[i] << " ";
                bfsq.push(ntmp->childs[i]);
            }
            bfsq.push(ntmp->childs[ntmp->total_child - 1]);
        }
        cout << endl;
        bfsq.pop();
    }
}

/*
 1.模版类友元问题
 2.stl lowerbound 如果没有元素的话会报错,返回>=key的第一个元素
 insert,assign
 5.实现b+树的物理保存加载代码
 6.b+树析构时 按合理顺序析构各个节点问题
 */

#endif /* bplustree_h */
