//
// Created by 哈哈泽 on 2018/6/15.
//

#ifndef MINISQL_BUFFERMANAGER_BUFFERMANAGER_H
#define MINISQL_BUFFERMANAGER_BUFFERMANAGER_H

#include <string>

namespace BufferManager {
    class Buffer {
    public:
        typedef char Byte;
        typedef std::string FileNameType;
        typedef int FileBlockIdType;
        typedef int BufferBlockIdType;

        Buffer() {
            buffer = new BufferBlock[MAX_BLOCK_NUM];
        }

        ~Buffer() {
            for (BufferBlockIdType id = 0; id < MAX_BLOCK_NUM; ++id) {
                if (!buffer[id].isAvail() && buffer[id].isModified())
                    output(id, buffer[id].getFileName(), buffer[id].getFileBlockId());
            }
            delete[] buffer;
        }

        class BufferBlock;

        BufferBlock *getBlock(const FileNameType &fileName, const FileBlockIdType &fileBlockId);

        BufferBlock *getNextBlock(BufferBlock* const & bufferBlockPtr);

        int getBlockNum(const FileNameType& fileName);

        void newFile(const FileNameType& newFileName);

        void deleteFile(const FileNameType &fileName);

        void addBlock(BufferBlock* const & bufferBlockPtr);

        BufferBlock* addBlock(const FileNameType& fileName);

        void output(const BufferBlockIdType &bufferBlockId, const FileNameType &fileName,
                    const FileBlockIdType &fileBlockId);

        void input(const BufferBlockIdType &bufferBlockId, const FileNameType &fileName,
                   const FileBlockIdType &fileBlockId);

        BufferBlockIdType findAvailBufferBlock();

        static const unsigned MAX_BLOCK_NUM = 100;

        class BufferBlock {
        public:
            static const unsigned BLOCK_BYTE_SIZE = 4096; //4kB

            BufferBlock() : fileName(""), fileBlockId(-1), LRU(-1), tail(0),
                            pinned(false), modified(false), avail(true) {
                data = new Byte[BLOCK_BYTE_SIZE];
            }

            int tail;

            ~BufferBlock() {
                delete[] data;
            }

            /* User interface */
            bool updateBufferBlockData(const Byte newData[]);

            const Byte *readBufferBlockData()const;

            Byte *getBufferBlockDataHandle();

            /* Enclosing class interface */
            FileNameType getFileName()const;

            void setFileName(const FileNameType& newFileName);

            void setFileBlockId(const FileBlockIdType& newFileBlockId);

            FileBlockIdType getFileBlockId()const;

            int getLRU()const;

            void initLRU();

            void pin();

            void unPin();

            bool isPinned()const;

            void setModified();

            void setUnmodified();

            bool isModified()const;

            void setAvail();

            void setUnavail();

            bool isAvail()const;

            void incrementLRU();

            Byte *data; //Byte array(actual block)
            FileNameType fileName;
            FileBlockIdType fileBlockId;
            int LRU;
            bool pinned;
            bool modified;
            bool avail;
        };

        BufferBlock *buffer;
    };

    inline Buffer::Byte* Buffer::BufferBlock::getBufferBlockDataHandle()
    {
        return data;
    }

    inline Buffer::FileBlockIdType Buffer::BufferBlock::getFileBlockId()const
    {
        return fileBlockId;
    }

    inline Buffer::FileNameType Buffer::BufferBlock::getFileName()const
    {
        return fileName;
    }

    inline int Buffer::BufferBlock::getLRU()const {
        return LRU;
    }

    inline void Buffer::BufferBlock::initLRU() {
        LRU = 0;
    }

    inline void Buffer::BufferBlock::pin() {
        pinned = true;
    }

    inline void Buffer::BufferBlock::unPin() {
        pinned = false;
    }

    inline bool Buffer::BufferBlock::isPinned()const {
        return pinned;
    }

    inline void Buffer::BufferBlock::setModified() {
        modified = true;
    }

    inline void Buffer::BufferBlock::setUnmodified() {
        modified = false;
    }

    inline bool Buffer::BufferBlock::isModified()const {
        return modified;
    }

    inline void Buffer::BufferBlock::setAvail() {
        avail = true;
    }

    inline void Buffer::BufferBlock::setUnavail() {
        avail = false;
    }

    inline bool Buffer::BufferBlock::isAvail()const {
        return avail;
    }

    inline void Buffer::BufferBlock::incrementLRU() {
        ++LRU;
    }

    inline void Buffer::BufferBlock::setFileName(const FileNameType& newFileName){
        fileName = newFileName;
    }

    inline void Buffer::BufferBlock::setFileBlockId(const FileBlockIdType& newFileBlockId) {
        fileBlockId = newFileBlockId;
    }

}
#endif //MINISQL_BUFFERMANAGER_BUFFERMANAGER_H
