//
// Created by 哈哈泽 on 2018/6/15.
//

#include "BufferManager.h"
#include <cstdlib>
#include "CatalogManager.h"

using namespace BufferManager;
extern CatalogManager CL;

Buffer::BufferBlock*
Buffer::getBlock(const FileNameType &fileName, const FileBlockIdType &fileBlockId)
{
    if(fileBlockId + 1 > CL.Get_Block_Num(fileName))
        return nullptr;
    /* Find block with identifier "fileBlockId" in file "fileName" in the buffer if exists */
    for(BufferBlockIdType id = 0; id < MAX_BLOCK_NUM; ++id)
    {
        if(buffer[id].getFileName() == fileName
           && buffer[id].getFileBlockId() == fileBlockId
                && !buffer[id].isAvail())
        {
            buffer[id].incrementLRU();
            buffer[id].setModified();
            return &buffer[id];
        }
    }
    /* If not exists, input from file (replace some buffer block if necessary) */
    BufferBlockIdType returnBufferBlockId = findAvailBufferBlock(); //employ LRU replacement strategy if necessary
    input(returnBufferBlockId, fileName, fileBlockId);
    buffer[returnBufferBlockId].setModified(); //assume being modified by user
    return &buffer[returnBufferBlockId];
}

Buffer::BufferBlock*
Buffer::getNextBlock(BufferBlock* const &bufferBlockPtr)
{
    return getBlock(bufferBlockPtr->getFileName(), bufferBlockPtr->getFileBlockId() + 1);
}

int Buffer::getBlockNum(const FileNameType& fileName)
{
    return CL.Get_Block_Num(fileName);
}

void
Buffer::addBlock(BufferBlock* const & bufferBlockPtr)
{
    BufferBlockIdType availBlockId = findAvailBufferBlock();
    buffer[availBlockId].setFileName(bufferBlockPtr->getFileName());
    buffer[availBlockId].setFileBlockId(bufferBlockPtr->getFileBlockId() + 1);
    buffer[availBlockId].initLRU(); //LRU <= 0
    buffer[availBlockId].unPin();
    buffer[availBlockId].setUnmodified();
    buffer[availBlockId].setUnavail();
    CL.Increase_Block_Num(bufferBlockPtr->getFileName());
}

Buffer::BufferBlock*
Buffer::addBlock(const FileNameType& fileName)
{
    BufferBlockIdType availBlockId = findAvailBufferBlock();
    buffer[availBlockId].setFileName(fileName);
    buffer[availBlockId].setFileBlockId(CL.Get_Block_Num(fileName));
    buffer[availBlockId].initLRU(); //LRU <= 0
    buffer[availBlockId].unPin();
    buffer[availBlockId].setModified();
    buffer[availBlockId].setUnavail();
    CL.Increase_Block_Num(fileName);
    return &buffer[availBlockId];
}

void
Buffer::newFile(const FileNameType &newFileName)
{
    CL.Set_Block_Num(newFileName, 0);
}

void
Buffer::deleteFile(const FileNameType &fileName)
{
    for(BufferBlockIdType id = 0; id < MAX_BLOCK_NUM; ++id)
    {
        if(buffer[id].getFileName() == fileName && !buffer[id].isAvail())
        {
            buffer[id].setAvail();
        }
    }
    CL.Set_Block_Num(fileName, 0);
    string path = "./DB_Files/" + CL.used_Database + "/" + fileName;
    remove(path.c_str());
}
void
Buffer::output(const BufferBlockIdType &bufferBlockId, const FileNameType &fileName,
                    const FileBlockIdType &fileBlockId)
{
    FILE *fptr = nullptr;
    fptr = fopen(string("./DB_Files/" + CL.Get_Used_Database() + "/" + fileName).c_str(), "r+");
    if(fptr == nullptr)
        fptr = fopen(string("./DB_Files/" + CL.Get_Used_Database() + "/" + fileName).c_str(), "w+");
    if(fptr != nullptr)
    {
        if(fseek(fptr, fileBlockId * BufferBlock::BLOCK_BYTE_SIZE, 0) == 0)
        {
            fwrite(buffer[bufferBlockId].getBufferBlockDataHandle(), BufferBlock::BLOCK_BYTE_SIZE, 1, fptr);
        }
        fclose(fptr);
    }
    else
        cout << "Error!!!!!!" << endl;
    buffer[bufferBlockId].setAvail();
}

void
Buffer::input(const BufferBlockIdType &bufferBlockId, const FileNameType &fileName,
                   const FileBlockIdType &fileBlockId)
{
    FILE *fptr = nullptr;
    if((fptr = fopen(string("./DB_Files/" + CL.Get_Used_Database() + "/" + fileName).c_str(), "r")) != nullptr)
    {
        if(fseek(fptr, fileBlockId * BufferBlock::BLOCK_BYTE_SIZE, 0) == 0)
        {
            fread(buffer[bufferBlockId].getBufferBlockDataHandle(), BufferBlock::BLOCK_BYTE_SIZE, 1, fptr);
        }
        fclose(fptr);
    }

    buffer[bufferBlockId].setFileName(fileName);
    buffer[bufferBlockId].setFileBlockId(fileBlockId);
    buffer[bufferBlockId].initLRU(); //LRU <= 0
    buffer[bufferBlockId].unPin();
    buffer[bufferBlockId].setUnmodified();
    buffer[bufferBlockId].setUnavail();
}

Buffer::BufferBlockIdType
Buffer::findAvailBufferBlock()
{
    int minLRU = INT8_MAX;
    BufferBlockIdType minLRUBufferBlockId = -1;
    for(BufferBlockIdType id = 0; id < MAX_BLOCK_NUM; ++id)
    {
        if(buffer[id].isAvail())
            return id;
        if(!buffer[id].isPinned() && buffer[id].getLRU() < minLRU)
        {
            minLRU = buffer[id].getLRU();
            minLRUBufferBlockId = id;
        }
    }
    if(buffer[minLRUBufferBlockId].isModified())
        output(minLRUBufferBlockId, buffer[minLRUBufferBlockId].getFileName(), buffer[minLRUBufferBlockId].getFileBlockId());
    return minLRUBufferBlockId;
}

bool
Buffer::BufferBlock::updateBufferBlockData(const Byte *newData)
{
    for(int i = 0; i < BLOCK_BYTE_SIZE; ++i)
    {
        data[i] = newData[i];
    }
    return true;
}

const Buffer::Byte*
Buffer::BufferBlock::readBufferBlockData()const
{
    return data;
}
