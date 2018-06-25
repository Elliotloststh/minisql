#include "RecordManager.hpp"
#include <sstream>
#include "CatalogManager.h"
#include "IndexManager.h"



using namespace std;
using namespace BufferManager;
using Block = Buffer::BufferBlock;

extern Buffer bm;
extern CatalogManager CL;
extern IndexManager IM;

int RecordManager::Record_InsertValue(const string table_name, vector<string>& value_vec, vector<Attribute>& Attribute_vec)
{
    if(!able_to_insert(table_name, Attribute_vec, value_vec))
        return -1;
    insert_index(table_name, Attribute_vec, value_vec);
    Block *chunk = bm.getBlock(table_name, 0);
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    
    int lastid;
    int tmp;
    memcpy(&tmp, chunk->data, 4);
    chunk->tail = tmp;
    while(chunk->tail < 0)
    {
        chunk = bm.getNextBlock(chunk);
        memcpy(&tmp, chunk->data, 4);
        chunk->tail = tmp;
    }
    lastid = chunk->getFileBlockId();
    chunk->pin();
    
    int total_attr_size = get_TotalLength(Attribute_vec);
    
    //BYTE *buffer = new BYTE[total_attr_size];
    
    int offset = 0;
    
    for(int i = 0; i < value_vec.size(); ++i)  
    {  

       	stringstream ss;  
 		ss<<value_vec[i];
       	switch(Attribute_vec[i].attr_type)
       	{
       		case INT:
       			int inttmp;
       			ss>>inttmp;
       			memcpy(chunk->data+chunk->tail+4, &inttmp, Attribute_vec[i].attr_len);
                chunk->tail+=Attribute_vec[i].attr_len;
                memcpy(chunk->data, &(chunk->tail), 4);
//                cout<<Attribute_vec[i].attr_len<<inttmp<<endl;
       			break;
       		case FLOAT:
       			float floattmp;
       			ss>>floattmp;
       			memcpy(chunk->data+chunk->tail+4, &floattmp, Attribute_vec[i].attr_len);
                chunk->tail+=Attribute_vec[i].attr_len;
                memcpy(chunk->data, &(chunk->tail), 4);
//                cout<<Attribute_vec[i].attr_len<<floattmp<<endl;
       			break;
       		case CHAR:
       			memcpy(chunk->data+chunk->tail+4, value_vec[i].c_str(), Attribute_vec[i].attr_len);
                chunk->tail+=Attribute_vec[i].attr_len;
                memcpy(chunk->data, &(chunk->tail), 4);
//                cout<<Attribute_vec[i].attr_len<<value_vec[i].c_str()<<endl;
       			break;
       	}
       	offset+=Attribute_vec[i].attr_len;
    }

    //如果块空间不足，转到下一个块
    int tail_addr =chunk->tail+4;
    if( BLOCKSIZE <= tail_addr + total_attr_size)
    {
        chunk->tail = -chunk->tail;
        chunk->unPin();
        
    	chunk = bm.getNextBlock(chunk);
        if(chunk==NULL)
        {
            chunk = bm.addBlock(table_name);
            int t = 0;
            memcpy(chunk->data, &t, 4);
        }
        chunk->pin();
    	tail_addr = 4;
    }
    
    //重新写入块尾
//    chunk->tail = tail_addr-4;

    chunk->unPin();
    
    
    
	return tail_addr;
}


int RecordManager::Record_DeleteAllValue(const string table_name, vector<Attribute>& Attribute_vec)
{
    Block* chunk = bm.getBlock(table_name, 0);
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    
    chunk->pin();
    chunk->tail = 0;
    memcpy(chunk->data, &(chunk->tail), 4);
    chunk->unPin();
    vector<string> index_attr_name = CL.Get_Table_Index(table_name);
    int attr_no;
    
    int type;
    
    for(int i = 0; i < index_attr_name.size()/2; i=i+2)
    {
        for(int j = 0; j < Attribute_vec.size(); ++j)
        {
            if(index_attr_name[i*2] == Attribute_vec[j].attr_name)
            {
                attr_no = j;
                break;
            }
        }
        switch(Attribute_vec[attr_no].attr_type)
        {
            case(INT):
                type = 0;
                break;
            case(FLOAT):
                type = -1;
                break;
            case(CHAR):
                type = Attribute_vec[attr_no].attr_len;
                break;
        }
        IM.deleteAll(CL.Get_Used_Database(), table_name, index_attr_name[i], index_attr_name[i+1], type);
    }
    return get_Record_Size(table_name, Attribute_vec)+1;
}


int RecordManager::Record_DeleteValue(const string table_name, vector<Condition>& Condition_vec, vector<Attribute>& Attribute_vec)
{
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
//    if(chunk->tail==0)
//        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    int total_attr_size = get_TotalLength(Attribute_vec);
    int ret=0;
    int query_addr = 4;
    int count=-1;
    //遍历所有记录
    while(!meet_tail(query_addr, chunk, table_name))
    {
        
    	//判断是否满足条件
    	bool flag = true;
    	int i, j;
        count+=1;
        BYTE* buffer2 = new BYTE[total_attr_size];
    	for(i = 0; i < Condition_vec.size(); i++)
    	{
    		stringstream ss;  
 			int offset = 0;  		
    		int inttmp;
    		float floattmp;
    		char* chtmp;
    		string strtmp;
    		int condition_type;
    		int length;
    		for(j = 0; j < Attribute_vec.size(); ++j)  
  			{
    			if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
    			{
    				condition_type = Attribute_vec[j].attr_type; 
    				length = Attribute_vec[j].attr_len;
    				chtmp = new char[length+1];
    				break;
    			}
    			else
    				offset+=Attribute_vec[j].attr_len;
    		}
    		switch(condition_type)
    		{
    			case INT:
    				memcpy(&inttmp, chunk->data+query_addr+offset, length);
    				flag = Condition_vec[i].ifRight(inttmp);
                    break;
    			case FLOAT:
    				memcpy(&floattmp, chunk->data+query_addr+offset, length);
    				flag = Condition_vec[i].ifRight(floattmp);
                    break;
    			case CHAR:
    				memcpy(chtmp, chunk->data+query_addr+offset, length);
    				chtmp[length] = '\0';
    				strtmp = string(chtmp);
    				flag = Condition_vec[i].ifRight(strtmp);
    				break;
    		}
    		if(!flag)
    			break;
    	}
    	//execute delete operation
  		if(flag)
  		{
            ret++;
            Block *blocktmp = bm.getBlock(table_name, 0);
            BYTE* buffer = new BYTE[total_attr_size];
            int lastid;
            memcpy(&(blocktmp->tail), chunk->data, 4);
            while(blocktmp->tail < 0)
            {
                blocktmp = bm.getNextBlock(blocktmp);
                memcpy(&(blocktmp->tail), chunk->data, 4);
            }
            lastid = blocktmp->getFileBlockId();
            
            
            int tail_addr = blocktmp->tail+4;
            
  			if(tail_addr == 4)
  			{
                blocktmp = bm.getBlock(table_name, lastid-1);
                memcpy(&(blocktmp->tail), blocktmp->data, 4);
  				tail_addr = -blocktmp->tail;
  			}

  			tail_addr-=total_attr_size;

  			//BYTE *buffer = new BYTE[total_attr_size];
  			memcpy(buffer, blocktmp->data+tail_addr, total_attr_size);
            memcpy(buffer2, chunk->data+query_addr, total_attr_size);
  			memcpy(chunk->data+query_addr, blocktmp->data+tail_addr,total_attr_size);
            blocktmp->tail -= total_attr_size;
            memcpy(blocktmp->data, &(blocktmp->tail), 4);
            update_index_tail(table_name, Attribute_vec, count, buffer);
            delete_index(table_name, Attribute_vec, buffer2);
  		}
        if(!flag)
            query_addr+=total_attr_size;
//        cout<<query_addr<<" "<<total_attr_size<<endl;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
    }
    chunk->unPin();
    return ret;
}

int RecordManager::Record_Update(string table_name, vector<Attribute>& select_attr_vec, vector<string>& value_vec,
                                 vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec)
{
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
    //    if(chunk->tail==0)
    //        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    int total_attr_size = get_TotalLength(Attribute_vec);
    int ret = 0;
    int query_addr = 4;
    BYTE* buffer2 = new BYTE[total_attr_size];
    
    while(!meet_tail(query_addr, chunk, table_name))
    {
        int i,j;
        bool flag = true;
        for(i = 0; i < Condition_vec.size(); ++i)
        {
            stringstream ss;
            int offset = 0;
            int inttmp;
            float floattmp;
            char* chtmp;
            string strtmp;
            int condition_type;
            int length;
            for(j = 0; j < Attribute_vec.size(); ++j)
            {
                if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
                {
                    condition_type = Attribute_vec[j].attr_type;
                    length = Attribute_vec[j].attr_len;
                    chtmp = new char[length+1];
                    break;
                }
                else
                    offset+=Attribute_vec[j].attr_len;
            }
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
            if(!flag)
                break;
        }
        
        if(flag)
        {
            ret++;
        }
        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
        
    }
    chunk->unPin();
    if(ret==0)
        return 0;
    else if (ret > 1 && (select_attr_vec[0].attr_key_type==PRIMARY || select_attr_vec[0].attr_key_type==EMPTY))
        return -1;
    
    
    bid = 0;
    chunk = bm.getBlock(table_name, bid);
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    query_addr = 4;
    ret = 0;
    //    int offset;
    //遍历所有记录
    int backupint;
    int backupfloat;
    string backupstr;
    stringstream backupss;
    while(!meet_tail(query_addr, chunk, table_name))
    {
        int i,j;
        bool flag = true;
        for(i = 0; i < Condition_vec.size(); ++i)
        {
            stringstream ss;
            int offset = 0;
            int inttmp;
            float floattmp;
            char* chtmp;
            string strtmp;
            int condition_type;
            int length;
            for(j = 0; j < Attribute_vec.size(); ++j)
            {
                if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
                {
                    condition_type = Attribute_vec[j].attr_type;
                    length = Attribute_vec[j].attr_len;
                    chtmp = new char[length+1];
                    break;
                }
                else
                    offset+=Attribute_vec[j].attr_len;
            }
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
            if(!flag)
                break;
        }
        
        if(flag)
        {
            ret++;
            int offset = 0;
            int i, j;
            int count = 0;
            for(i = 0; i < Attribute_vec.size() && count < select_attr_vec.size(); ++i)
            {
                for(j = 0; j < select_attr_vec.size(); ++j)
                {
                    if(Attribute_vec[i].attr_id == select_attr_vec[j].attr_id)
                    {
                        count++;
                        stringstream ss;
                        ss<<value_vec[j];
                        memcpy(buffer2, chunk->data+query_addr, total_attr_size);
                        switch(select_attr_vec[j].attr_type)
                        {
                            case INT:
                                int inttmp;
                                ss>>inttmp;
                                memcpy(&backupint, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupss<<backupint;
                                memcpy(chunk->data+query_addr+offset, &inttmp, select_attr_vec[j].attr_len);
                                break;
                            case FLOAT:
                                float floattmp;
                                ss>>floattmp;
                                memcpy(&backupfloat, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupss<<backupfloat;
                                memcpy(chunk->data+query_addr+offset,&floattmp, select_attr_vec[j].attr_len);
                                break;
                            case CHAR:
                                memcpy(chunk->data+query_addr+offset, value_vec[j].c_str(), select_attr_vec[j].attr_len);
                                char* backupch = new char[select_attr_vec[j].attr_len+1];
                                memcpy(backupch, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupch[select_attr_vec[j].attr_len] = '\0';
                                backupstr = string(backupch);
                                backupss<<backupstr;
                                break;
                        }
                        update_index(table_name,Attribute_vec,select_attr_vec[j],value_vec[j], buffer2);
                        if(select_attr_vec[j].attr_key_type==PRIMARY || select_attr_vec[j].attr_key_type==EMPTY)
                        {
                            SelectResult SR;
                            vector<Condition> cv;
                            Condition c(select_attr_vec[j].attr_name, value_vec[j], 0);
                            cv.push_back(c);
                            if(Record_SelectValue(table_name, select_attr_vec, SR, Attribute_vec, cv)==2)
                            {
                                BYTE* buffer3 = new BYTE[total_attr_size];
                                memcpy(buffer3, chunk->data+query_addr, total_attr_size);
                                update_index(table_name,Attribute_vec,select_attr_vec[j],backupss.str(), buffer3);
                                switch(select_attr_vec[j].attr_type)
                                {
                                    case INT:
                                        memcpy(chunk->data+query_addr+offset, &backupint, select_attr_vec[j].attr_len);
                                        break;
                                    case FLOAT:
                                        float floattmp;
                                        memcpy(chunk->data+query_addr+offset,&backupfloat, select_attr_vec[j].attr_len);
                                        break;
                                    case CHAR:
                                        memcpy(chunk->data+query_addr+offset, backupstr.c_str(), select_attr_vec[j].attr_len);
                                        break;
                                }
                                chunk->unPin();
                                return -1;
                            }
                        }
                        
                    }
                }
                offset+=Attribute_vec[i].attr_len;
            }
        }
        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
        
    }
    chunk->unPin();
    return ret;
}

int RecordManager::Record_Update(const string table_name, vector<int>& offset, vector<Attribute>& select_attr_vec, vector<string>& value_vec, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec)
{
    int ret = 0;
    int total_attr_size = get_TotalLength(Attribute_vec);
    BYTE* buffer2 = new BYTE[total_attr_size];
    int capicity = (BLOCKSIZE-4)/total_attr_size;
    int BlockIndex;
    for(int k = 0; k < offset.size(); ++k)
    {
        BlockIndex = offset[k]/capicity;
        Block* chunk = bm.getBlock(table_name, BlockIndex);
        chunk->pin();
        int query_addr = (offset[k] - BlockIndex*capicity)*total_attr_size+4;
//        int offset = 0;
        int i,j;
        bool flag = true;
        for(i = 0; i < Condition_vec.size(); ++i)
        {
            stringstream ss;
            int offset = 0;
            int inttmp;
            float floattmp;
            char* chtmp;
            string strtmp;
            int condition_type;
            int length;
            for(j = 0; j < Attribute_vec.size(); ++j)
            {
                if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
                {
                    condition_type = Attribute_vec[j].attr_type;
                    length = Attribute_vec[j].attr_len;
                    chtmp = new char[length+1];
                    break;
                }
                else
                    offset+=Attribute_vec[j].attr_len;
            }
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
            if(!flag)
                break;
        }
        if(flag)
        {
            ret++;
        }
        chunk->unPin();
    }
    if(ret==0)
        return 0;
    else if (ret > 1 && (select_attr_vec[0].attr_key_type==PRIMARY || select_attr_vec[0].attr_key_type==EMPTY))
        return -1;
    
    ret = 0;
    //    int offset;
    //遍历所有记录
    int backupint;
    int backupfloat;
    string backupstr;
    stringstream backupss;
    for(int k = 0; k < offset.size(); ++k)
    {
        BlockIndex = offset[k]/capicity;
        Block* chunk = bm.getBlock(table_name, BlockIndex);
        chunk->pin();
        int query_addr = (offset[k] - BlockIndex*capicity)*total_attr_size+4;
        //        int offset = 0;
        int i,j;
        bool flag = true;
        for(i = 0; i < Condition_vec.size(); ++i)
        {
            stringstream ss;
            int offset = 0;
            int inttmp;
            float floattmp;
            char* chtmp;
            string strtmp;
            int condition_type;
            int length;
            for(j = 0; j < Attribute_vec.size(); ++j)
            {
                if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
                {
                    condition_type = Attribute_vec[j].attr_type;
                    length = Attribute_vec[j].attr_len;
                    chtmp = new char[length+1];
                    break;
                }
                else
                    offset+=Attribute_vec[j].attr_len;
            }
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
            if(!flag)
                break;
        }
        if(flag)
        {
            ret++;
            int offset = 0;
            int i, j;
            int count = 0;
            for(i = 0; i < Attribute_vec.size() && count < select_attr_vec.size(); ++i)
            {
                for(j = 0; j < select_attr_vec.size(); ++j)
                {
                    if(Attribute_vec[i].attr_id == select_attr_vec[j].attr_id)
                    {
                        count++;
                        stringstream ss;
                        ss<<value_vec[j];
                        memcpy(buffer2, chunk->data+query_addr, total_attr_size);
                        switch(select_attr_vec[j].attr_type)
                        {
                            case INT:
                                int inttmp;
                                ss>>inttmp;
                                memcpy(&backupint, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupss<<backupint;
                                memcpy(chunk->data+query_addr+offset, &inttmp, select_attr_vec[j].attr_len);
                                break;
                            case FLOAT:
                                float floattmp;
                                ss>>floattmp;
                                memcpy(&backupfloat, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupss<<backupfloat;
                                memcpy(chunk->data+query_addr+offset,&floattmp, select_attr_vec[j].attr_len);
                                break;
                            case CHAR:
                                memcpy(chunk->data+query_addr+offset, value_vec[j].c_str(), select_attr_vec[j].attr_len);
                                char* backupch = new char[select_attr_vec[j].attr_len+1];
                                memcpy(backupch, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                backupch[select_attr_vec[j].attr_len] = '\0';
                                backupstr = string(backupch);
                                backupss<<backupstr;
                                break;
                        }
                        update_index(table_name,Attribute_vec,select_attr_vec[j],value_vec[j], buffer2);
                        if(select_attr_vec[j].attr_key_type==PRIMARY || select_attr_vec[j].attr_key_type==EMPTY)
                        {
                            SelectResult SR;
                            vector<Condition> cv;
                            Condition c(select_attr_vec[j].attr_name, value_vec[j], 0);
                            cv.push_back(c);
                            
                            if(Record_SelectValue(table_name, select_attr_vec, SR, Attribute_vec, cv)==2)
                            {
                                BYTE* buffer3 = new BYTE[total_attr_size];
                                memcpy(buffer3, chunk->data+query_addr, total_attr_size);
                                update_index(table_name,Attribute_vec,select_attr_vec[j],backupss.str(), buffer3);
                                switch(select_attr_vec[j].attr_type)
                                {
                                    case INT:
                                        memcpy(chunk->data+query_addr+offset, &backupint, select_attr_vec[j].attr_len);
                                        break;
                                    case FLOAT:
                                        memcpy(chunk->data+query_addr+offset,&backupfloat, select_attr_vec[j].attr_len);
                                        break;
                                    case CHAR:
                                        memcpy(chunk->data+query_addr+offset, backupstr.c_str(), select_attr_vec[j].attr_len);
                                        break;
                                }
                                chunk->unPin();
                                return -1;
                            }
                        }
                        
                    }
                }
                offset+=Attribute_vec[i].attr_len;
            }
            
        }
        chunk->unPin();
    }
    
    return ret;
}

int RecordManager::Record_SelectAll(const string table_name, vector<Attribute>& select_attr_vec,
	SelectResult& result, vector<Attribute>& Attribute_vec)
{
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
    //    if(chunk->tail==0)
    //        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    vector<string> one_Record;
    int total_attr_size = get_TotalLength(Attribute_vec);
    int query_addr = 4;
    while(!meet_tail(query_addr, chunk, table_name))
    {
    	one_Record.clear();
    	int offset = 0;
    	int i, j;
    	int count = 0;
    	for(i = 0; i < Attribute_vec.size() && count < select_attr_vec.size(); ++i)
    	{
    		for(j = 0; j < select_attr_vec.size(); ++j)
    		{
    			if(Attribute_vec[i].attr_id == select_attr_vec[j].attr_id)
    			{
    				count++;  				
    				stringstream ss;
    				switch(select_attr_vec[j].attr_type)
    				{
    					case INT:
    						int inttmp;
    						memcpy(&inttmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
    						ss<<inttmp;
    						break;
    					case FLOAT:
    						float floattmp;
    						memcpy(&floattmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
    						ss<<floattmp;
    						break;
    					case CHAR:
    						char* chtmp = new char[select_attr_vec[j].attr_len+1];
    						memcpy(chtmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
    						chtmp[select_attr_vec[j].attr_len] = '\0';
    						string strtmp = string(chtmp);
    						ss<<strtmp;
    						break;
    				}

    				one_Record.push_back(ss.str());
    			}
    		}
    		offset+=Attribute_vec[i].attr_len;
    	}
    	result.push_back(one_Record);

        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
//        int blockIndex = (query_addr + total_attr_size) / BLOCKSIZE;
//
//        if((blockIndex + 1) * BLOCKSIZE < query_addr + total_attr_size)
//        {
//            chunk = bm.NextBlock(chunk);
//            query_addr = bm.getbeginAddress(chunk);
//        }

    }

    chunk->unPin();
    return get_Record_Size(table_name, Attribute_vec)+1;
}


int RecordManager::Record_SelectValue(const string table_name, vector<Attribute>& select_attr_vec,
    	SelectResult& result, vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec)
{
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
    //    if(chunk->tail==0)
    //        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    int total_attr_size = get_TotalLength(Attribute_vec);
    int ret = 0;
    int query_addr = 4;
    vector<string> one_Record;

    while(!meet_tail(query_addr, chunk, table_name))
    {
    	int i,j;
    	bool flag = true;
    	for(i = 0; i < Condition_vec.size(); ++i)
    	{
    		stringstream ss;  
 			int offset = 0;  		
    		int inttmp;
    		float floattmp;
    		char* chtmp;
    		string strtmp;
    		int condition_type;
    		int length;
    		for(j = 0; j < Attribute_vec.size(); ++j)  
  			{
    			if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
    			{
    				condition_type = Attribute_vec[j].attr_type; 
    				length = Attribute_vec[j].attr_len;
    				chtmp = new char[length+1];
    				break;
    			}
    			else
    				offset+=Attribute_vec[j].attr_len;
    		}
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
    		if(!flag)
    			break;
    	}

    	if(flag)
    	{
            ret++;
    		one_Record.clear();
   		 	int offset = 0;
    		int i, j;
    		int count = 0;
    		for(i = 0; i < Attribute_vec.size() && count < select_attr_vec.size(); ++i)
    		{
    			for(j = 0; j < select_attr_vec.size(); ++j)
    			{
    				if(Attribute_vec[i].attr_id == select_attr_vec[j].attr_id)
    				{
    					count++;  				
    					stringstream ss;
                        switch(select_attr_vec[j].attr_type)
                        {
                            case INT:
                                int inttmp;
                                memcpy(&inttmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                ss<<inttmp;
                                break;
                            case FLOAT:
                                float floattmp;
                                memcpy(&floattmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                ss<<floattmp;
                                break;
                            case CHAR:
                                char* chtmp = new char[select_attr_vec[j].attr_len+1];
                                memcpy(chtmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                chtmp[select_attr_vec[j].attr_len] = '\0';
                                string strtmp = string(chtmp);
                                ss<<strtmp;
                                break;
                        }

    					one_Record.push_back(ss.str());
    				}
    			}
    			offset+=Attribute_vec[i].attr_len;
    		}
    		result.push_back(one_Record);
    	}
        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }

    }
    chunk->unPin();
    return ret;
}

int RecordManager::Record_SelectValue(const string table_name, vector<int>& offset, vector<Attribute>& select_attr_vec,
                                      SelectResult& result,vector<Attribute>& Attribute_vec, vector<Condition>& Condition_vec)
{
    int total_attr_size = get_TotalLength(Attribute_vec);
    int capicity = (BLOCKSIZE-4)/total_attr_size;
    int BlockIndex;
    vector<string> one_Record;
    for(int k = 0; k < offset.size(); ++k)
    {
        BlockIndex = offset[k]/capicity;
        Block* chunk = bm.getBlock(table_name, BlockIndex);
        chunk->pin();
        int query_addr = (offset[k] - BlockIndex*capicity)*total_attr_size+4;
        one_Record.clear();
//        int offset = 0;
        int i,j;
        bool flag = true;
        for(i = 0; i < Condition_vec.size(); ++i)
        {
            stringstream ss;
            int offset = 0;
            int inttmp;
            float floattmp;
            char* chtmp;
            string strtmp;
            int condition_type;
            int length;
            for(j = 0; j < Attribute_vec.size(); ++j)
            {
                if(Condition_vec[i].attributeName == Attribute_vec[j].attr_name)
                {
                    condition_type = Attribute_vec[j].attr_type;
                    length = Attribute_vec[j].attr_len;
                    chtmp = new char[length+1];
                    break;
                }
                else
                    offset+=Attribute_vec[j].attr_len;
            }
            switch(condition_type)
            {
                case INT:
                    memcpy(&inttmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(inttmp);
                    break;
                case FLOAT:
                    memcpy(&floattmp, chunk->data+query_addr+offset, length);
                    flag = Condition_vec[i].ifRight(floattmp);
                    break;
                case CHAR:
                    memcpy(chtmp, chunk->data+query_addr+offset, length);
                    chtmp[length] = '\0';
                    strtmp = string(chtmp);
                    flag = Condition_vec[i].ifRight(strtmp);
                    break;
            }
            if(!flag)
                break;
        }
        if(flag)
        {
            int offset = 0;
            int count = 0;
            for(i = 0; i < Attribute_vec.size() && count < select_attr_vec.size(); ++i)
            {
                for(j = 0; j < select_attr_vec.size(); ++j)
                {
                    if(Attribute_vec[i].attr_id == select_attr_vec[j].attr_id)
                    {
                        count++;
                        stringstream ss;
                        switch(select_attr_vec[j].attr_type)
                        {
                            case INT:
                                int inttmp;
                                memcpy(&inttmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                ss<<inttmp;
                                break;
                            case FLOAT:
                                float floattmp;
                                memcpy(&floattmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                ss<<floattmp;
                                break;
                            case CHAR:
                                char* chtmp = new char[select_attr_vec[j].attr_len+1];
                                memcpy(chtmp, chunk->data+query_addr+offset, select_attr_vec[j].attr_len);
                                chtmp[select_attr_vec[j].attr_len] = '\0';
                                string strtmp = string(chtmp);
                                ss<<strtmp;
                                break;
                        }
                        
                        one_Record.push_back(ss.str());
                    }
                }
                offset+=Attribute_vec[i].attr_len;
            }
            result.push_back(one_Record);
        }
        chunk->unPin();
    }
    return result.size();
}

void RecordManager::Record_CreateIndex(string table_name, string attribute_name, string index_name, vector<Attribute>& Attribute_vec)
{
    string path = CL.used_Database + "+" + table_name + "+" + attribute_name + "+" + index_name;
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
    //    if(chunk->tail==0)
    //        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    int total_attr_size = get_TotalLength(Attribute_vec);
    int ret=0;
    int query_addr = 4;
    int count=-1;
    int attr_no;
    int offset = 0;
    for(int i = 0; i < Attribute_vec.size(); ++i)
    {
        if(Attribute_vec[i].attr_name==attribute_name)
        {
            attr_no = i;
            break;
        }
        offset+=Attribute_vec[i].attr_len;
    }
    int type;
    switch(Attribute_vec[attr_no].attr_type)
    {
        case(INT):
            type = 0;
            break;
        case(FLOAT):
            type = -1;
            break;
        case(CHAR):
            type = Attribute_vec[attr_no].attr_len;
            break;
    }
    //遍历所有记录
    while(!meet_tail(query_addr, chunk, table_name))
    {
        count++;
        stringstream ss;
        int inttmp;
        float floattmp;
        string strtmp;
        char* chtmp = new char[Attribute_vec[attr_no].attr_len+1];
        switch(Attribute_vec[attr_no].attr_type)
        {
            case(INT):
                memcpy(&inttmp, chunk->data+query_addr+offset, Attribute_vec[attr_no].attr_len);
                ss<<inttmp;
                break;
            case(FLOAT):
                memcpy(&floattmp, chunk->data+query_addr+offset, Attribute_vec[attr_no].attr_len);
                ss<<floattmp;
                break;
            case(CHAR):
                memcpy(chtmp, chunk->data+query_addr+offset, Attribute_vec[attr_no].attr_len);
                chtmp[Attribute_vec[attr_no].attr_len] = '\0';
                strtmp = string(chtmp);
                ss<<strtmp;
                break;
        }
        IM.insertOne(path, ss.str(), type, count);
        
        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
    }
    chunk->unPin();
}

bool RecordManager::able_to_insert(string table_name, vector<Attribute>& Attribute_vec, vector<string>& value_vec)
{
    int bid = 0;
    Block* chunk = bm.getBlock(table_name, bid);
    //    if(chunk->tail==0)
    //        return;
    if(chunk == NULL)
    {
        chunk = bm.addBlock(table_name);
        int t = 0;
        memcpy(chunk->data, &t, 4);
    }
    chunk->pin();
    int total_attr_size = get_TotalLength(Attribute_vec);
    int query_addr = 4;
    int offset;
    //遍历所有记录
    while(!meet_tail(query_addr, chunk, table_name))
    {
        offset = 0;
        stringstream ss;
        for(int i = 0; i < Attribute_vec.size(); ++i)
        {
            ss<<value_vec[i];
            if(Attribute_vec[i].attr_key_type==PRIMARY || Attribute_vec[i].attr_key_type==UNIQUE)
            {
                switch(Attribute_vec[i].attr_type)
                {
                    case INT:
                        int inttmp;
                        int inttmp2;
                        ss>>inttmp2;
                        memcpy(&inttmp, chunk->data+query_addr+offset, Attribute_vec[i].attr_len);
                        if(inttmp2 == inttmp)
                        {
                            chunk->unPin();
                            return false;
                        }
                        break;
                    case FLOAT:
                        float floattmp;
                        float floattmp2;
                        ss>>floattmp2;
                        memcpy(&floattmp, chunk->data+query_addr+offset, Attribute_vec[i].attr_len);
                        if(floattmp2 == floattmp)
                        {
                            chunk->unPin();
                            return false;
                        }
                        break;
                    case CHAR:
                        char* chtmp = new char[Attribute_vec[i].attr_len+1];
                        memcpy(chtmp, chunk->data+query_addr+offset, Attribute_vec[i].attr_len);
                        chtmp[Attribute_vec[i].attr_len] = '\0';
                        string strtmp = string(chtmp);
                        if(strtmp == value_vec[i])
                        {
                            chunk->unPin();
                            return false;
                        }
                        break;
                }
            }
            offset+=Attribute_vec[i].attr_len;
        }
        query_addr+=total_attr_size;
        if(BLOCKSIZE <= query_addr + total_attr_size)
        {
            chunk->unPin();
            chunk = bm.getNextBlock(chunk);
            memcpy(&(chunk->tail), chunk->data, 4);
            chunk->pin();
            query_addr = 4;
        }
    }
    chunk->unPin();
    return true;
}


void RecordManager::update_index(string table_name,vector<Attribute>& Attribute_vec,Attribute select_attr,string value, BYTE* buffer)
{
    int type;
    vector<string> index_attr_name = CL.Get_Table_Index(table_name);
    int attr_no;
    int offset=0;
    
    int inttmp;
    float floattmp;
    string strtmp;
    char* chtmp;
    stringstream ss;
    
    for(int i = 0; i < index_attr_name.size()/2; i=i+2)
    {
        for(int j = 0; j < Attribute_vec.size(); ++j)
        {
            if(index_attr_name[i*2] == Attribute_vec[j].attr_name)
            {
                attr_no = j;
                chtmp = new char[Attribute_vec[attr_no].attr_len+1];
                switch(Attribute_vec[attr_no].attr_type)
                {
                    case(INT):
                        type = 0;
                        memcpy(&inttmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<inttmp;
                        break;
                    case(FLOAT):
                        type = -1;
                        memcpy(&floattmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<floattmp;
                        break;
                    case(CHAR):
                        type = Attribute_vec[attr_no].attr_len;
                        memcpy(chtmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        chtmp[Attribute_vec[attr_no].attr_len]='\0';
                        strtmp = string(chtmp);
                        ss<<strtmp;
                        break;
                }
                break;
            }
            offset+=Attribute_vec[i].attr_len;
        }
        if(index_attr_name[i*2] == select_attr.attr_name)
        {
            string path = CL.used_Database + "+" + table_name + "+" + index_attr_name[i] + "+" + index_attr_name[i+1];
            int no = IM.searchOne(path, ss.str(), type);
            IM.deleteOneBykey(path, ss.str(), type);
            IM.insertOne(path, value, type, no);
        }
        
        
    }
    
}

void RecordManager::delete_index(string table_name, vector<Attribute>& Attribute_vec, BYTE* buffer)
{
    int type;
    vector<string> index_attr_name = CL.Get_Table_Index(table_name);
    int attr_no;
    int offset=0;
    //int total_attr_size = get_TotalLength(Attribute_vec);
    
    
    int inttmp;
    float floattmp;
    string strtmp;
    char* chtmp;
    stringstream ss;
    for(int i = 0; i < index_attr_name.size()/2; i=i+2)
    {
        offset = 0;
        for(int j = 0; j < Attribute_vec.size(); ++i)
        {
            if(index_attr_name[i*2] == Attribute_vec[j].attr_name)
            {
                attr_no = j;
                chtmp = new char[Attribute_vec[attr_no].attr_len+1];
                switch(Attribute_vec[attr_no].attr_type)
                {
                    case(INT):
                        type = 0;
                        memcpy(&inttmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<inttmp;
                        break;
                    case(FLOAT):
                        type = -1;
                        memcpy(&floattmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<floattmp;
                        break;
                    case(CHAR):
                        type = Attribute_vec[attr_no].attr_len;
                        memcpy(chtmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        chtmp[Attribute_vec[attr_no].attr_len]='\0';
                        strtmp = string(chtmp);
                        ss<<strtmp;
                        break;
                }
                break;
            }
            offset+=Attribute_vec[i].attr_len;
        }
        string path = CL.used_Database + "+" + table_name + "+" + index_attr_name[i] + "+" + index_attr_name[i+1];
        IM.deleteOneBykey(path, ss.str(), type);
    }
}

void RecordManager::update_index_tail(string table_name, vector<Attribute>& Attribute_vec, int count1, BYTE* buffer)
{
    int type;
    vector<string> index_attr_name = CL.Get_Table_Index(table_name);
    int attr_no;
    int offset=0;
    //int total_attr_size = get_TotalLength(Attribute_vec);
    
    
    int inttmp;
    float floattmp;
    string strtmp;
    char* chtmp;
    stringstream ss;
    
    for(int i = 0; i < index_attr_name.size()/2; i=i+2)
    {
        offset = 0;
        for(int j = 0; j < Attribute_vec.size(); ++i)
        {
            if(index_attr_name[i*2] == Attribute_vec[j].attr_name)
            {
                attr_no = j;
                chtmp = new char[Attribute_vec[attr_no].attr_len+1];
                switch(Attribute_vec[attr_no].attr_type)
                {
                    case(INT):
                        type = 0;
                        memcpy(&inttmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<inttmp;
                        break;
                    case(FLOAT):
                        type = -1;
                        memcpy(&floattmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        ss<<floattmp;
                        break;
                    case(CHAR):
                        type = Attribute_vec[attr_no].attr_len;
                        memcpy(chtmp, buffer+offset, Attribute_vec[attr_no].attr_len);
                        chtmp[Attribute_vec[attr_no].attr_len]='\0';
                        strtmp = string(chtmp);
                        ss<<strtmp;
                        break;
                }
                break;
            }
            offset+=Attribute_vec[i].attr_len;
        }
        string path = CL.used_Database + "+" + table_name + "+" + index_attr_name[i] + "+" + index_attr_name[i+1];
        IM.updateOne(path, ss.str(), type, count1);
    }
    
}

void RecordManager::insert_index(string table_name, vector<Attribute>& Attribute_vec, vector<string>& value_vec)
{
    int count = get_Record_Size(table_name, Attribute_vec);
    vector<string> index_attr_name = CL.Get_Table_Index(table_name);
    int attr_no;
    
    int type;

    for(int i = 0; i < index_attr_name.size()/2; i=i+2)
    {
        for(int j = 0; j < Attribute_vec.size(); ++j)
        {
            if(index_attr_name[i*2] == Attribute_vec[j].attr_name)
            {
                attr_no = j;
                break;
            }
        }
        switch(Attribute_vec[attr_no].attr_type)
        {
            case(INT):
                type = 0;
                break;
            case(FLOAT):
                type = -1;
                break;
            case(CHAR):
                type = Attribute_vec[attr_no].attr_len;
                break;
        }
        
        string path = CL.used_Database + "+" + table_name + "+" + index_attr_name[i] + "+" + index_attr_name[i+1];
        IM.insertOne(path, value_vec[attr_no], type, count+1);
        
    }
}

int RecordManager::get_Record_Size(string table_name, vector<Attribute>& Attribute_vec)
{
    int total_attr_size = get_TotalLength(Attribute_vec);
    int capicity = (BLOCKSIZE-4)/total_attr_size;
    int count = -1;
    Block *blocktmp = bm.getBlock(table_name, 0);
    if(blocktmp == NULL)
    {
        blocktmp = bm.addBlock(table_name);
        int t = 0;
        memcpy(blocktmp->data, &t, 4);
    }
    memcpy(&(blocktmp->tail), blocktmp->data, 4);
    while(blocktmp->tail < 0)
    {
        count+=capicity;
        blocktmp = bm.getNextBlock(blocktmp);
        memcpy(&(blocktmp->tail), blocktmp->data, 4);
    }
    count+=blocktmp->tail/total_attr_size;
    return count;
}

int RecordManager::get_TotalLength(vector<Attribute>& Attribute_vec)
{
    int total_attr_size = 0;
    for(int i = 0; i < Attribute_vec.size(); ++i)  
    {
        total_attr_size+=Attribute_vec[i].attr_len;
    }
    return total_attr_size;
}

bool RecordManager::meet_tail(int query_addr, Block* chunk, string table_name)
{
    memcpy(&(chunk->tail), chunk->data, 4);
//    cout<<query_addr<<endl;
    return query_addr==(chunk->tail+4)?true:false;
}
//--------------for class condition-------------// 

bool Condition::ifRight(int content)
{
    stringstream ss;
    ss << value;
    int myContent;
    ss >> myContent;
    
    switch (operate)
    {
        case Condition::OPERATOR_EQUAL:
            return content == myContent;
            break;
        case Condition::OPERATOR_NOT_EQUAL:
            return content != myContent;
            break;
        case Condition::OPERATOR_LESS:
            return content < myContent;
            break;
        case Condition::OPERATOR_MORE:
            return content > myContent;
            break;
        case Condition::OPERATOR_LESS_EQUAL:
            return content <= myContent;
            break;
        case Condition::OPERATOR_MORE_EQUAL:
            return content >= myContent;
            break;
        default:
            return true;
            break;
    }
}

bool Condition::ifRight(float content)
{
    stringstream ss;
    ss << value;
    float myContent;
    ss >> myContent;
    
    switch (operate)
    {
        case Condition::OPERATOR_EQUAL:
            return content == myContent;
            break;
        case Condition::OPERATOR_NOT_EQUAL:
            return content != myContent;
            break;
        case Condition::OPERATOR_LESS:
            return content < myContent;
            break;
        case Condition::OPERATOR_MORE:
            return content > myContent;
            break;
        case Condition::OPERATOR_LESS_EQUAL:
            return content <= myContent;
            break;
        case Condition::OPERATOR_MORE_EQUAL:
            return content >= myContent;
            break;
        default:
            return true;
            break;
    }
}

bool Condition::ifRight(string content)
{
    string myContent = value;
    switch (operate)
    {
        case Condition::OPERATOR_EQUAL:
            return content == myContent;
            break;
        case Condition::OPERATOR_NOT_EQUAL:
            return content != myContent;
            break;
        case Condition::OPERATOR_LESS:
            return content < myContent;
            break;
        case Condition::OPERATOR_MORE:
            return content > myContent;
            break;
        case Condition::OPERATOR_LESS_EQUAL:
            return content <= myContent;
            break;
        case Condition::OPERATOR_MORE_EQUAL:
            return content >= myContent;
            break;
        default:
            return true;
            break;
    }
}

Condition::Condition(string a,string v,int o) {
    attributeName = a;
    value = v;
    operate = o;
}

