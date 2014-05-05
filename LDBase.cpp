#include "LDBase.h"
#include <io.h>
LDPos LDBigIdlesBlock::getNowPos()
{
	unsigned long value = 2;
	if (now.value) value = sizeof(LDBHead) + (now.value - 1)* (sizeof(LDBigIdlesBlock) + sizeof(LDBigBlock));
	return LDPos(value);
}
LDPos LDBigBlock::getNowPos()
{
	unsigned long value = 2 + sizeof(LDBigIdlesBlock);
	if (index) value = sizeof(LDBHead) + (index -1) * (sizeof(LDBigIdlesBlock) + sizeof(LDBigBlock)) + sizeof(LDBigIdlesBlock);
	return LDPos(value);
}
LDBFile::LDBFile(const char *fileName)
{
	if (access(fileName,0) != 0)
	{
		 handle = fopen(fileName,"w+");
	}
	else handle = fopen(fileName,"r+");
}
bool LDBFile::format()
{
	moveTo(LDPos(0));
	head.reset();
	writeBlock(&head,sizeof(head));
	return true;
}
bool LDBFile::isDB()
{
	if (head.tag[0] == 'L' && head.tag[1]=='D')
	{
		if (!strncmp(head.version,"LDB1",4))
			return true;
	}
	return false;
}
void LDBFile::readHead()
{
	moveTo(LDPos(0));
	head.clear();
	readBlock(&head,sizeof(head));
}
bool LDBFile::open(bool withFormat)
{
	readHead();
	if (isDB())
	{
		return true;		
	}
	if (!withFormat)
		return false;
	if (format()) return true;
	return false;
}
/**
 * 写入文件块
 */
bool LDBFile::writeBlock(void *content,unsigned int len)
{
	if (!handle) return false;
	fwrite(content,len,1,handle);
	return true;
}

/**
 * 读取文件块
 */
bool LDBFile::readBlock(void *content,unsigned int len)
{
	if (!handle) return false;
	fread(content,len,1,handle);
	return true;
}


/**
 * 移动到某个点
 */
void LDBFile::moveTo(const LDPos &pos)
{
	if (!handle) return;
	fseek(handle,pos.value,SEEK_SET);
}

LDBFile::~LDBFile()
{
	if (handle)
		fclose(handle);
}


void LDBStream::write(LDBStream& ss,unsigned long tag)
{
	// 获取空闲子块
	LDBigBlock *nowBlock = head;
	LDBigBlock * pre = head;
	while (nowBlock && !nowBlock->checkTag(tag))
	{
		pre = nowBlock;
		nowBlock = getNext(nowBlock);
	}
	if (!nowBlock)
	{
		nowBlock = newNextBlock(pre);
		nowBlock->offset = tag / IDLES_BLOCK_SIZE;
	}
	if (nowBlock)
	{
		LDSmallBlock * block = nowBlock->getSmallBlock(tag);
		if (block)
		{
			block->tag = LDSmallBlock::STREAM;
			block->write(ss.head);
		}
	}
}

LDBigBlock * LDBStream::getNext(LDBigBlock * block)
{
	if (!_file) return NULL;
	if (block->next.pointer) return (LDBigBlock*)block->next.pointer;
	
	if (block->next.value) 
	{
		LDBigBlock *newBlock = new LDBigBlock;
		block->next.pointer = newBlock;
		_file->moveTo(block->next.value);
		_file->readBlock(newBlock,sizeof(LDBigBlock));
		newBlock->pre = block->getNowPos();
		newBlock->pre.pointer = block;
		return newBlock;
	}
	return NULL;
}

void LDBStream::save(int tag)
{
	_file->head.signal.start(LDBWriteSingal::WRITE);
	LDBigBlock *nowBlock  = head;
	while (nowBlock && !nowBlock->checkTag(tag))
	{
		nowBlock = getNext(nowBlock);
	}
	if (nowBlock)
	{
		LDSmallBlock* small = nowBlock->getSmallBlock(tag);
		if (small->tag == LDSmallBlock::STREAM)
		{
			LDBigBlock *bigBlock = (LDBigBlock*) small->value;
			writeSmallBlock(small); // 写入小块
			writeBigBlock(bigBlock); // 写入大块
		}
		else
		{
			writeSmallBlock(small); // 写入小块
		}
	}
	_file->head.clear();
	_file->updateWirteSignal();
}

/**
 * 创建下一个块
 */
LDBigBlock * LDBStream::newNextBlock(LDBigBlock *nowBlock)
{
	if (nowBlock->next.pointer) return (LDBigBlock*) nowBlock->next.pointer;
	LDBigBlock *newBlock = new LDBigBlock;
	nowBlock->next.pointer = newBlock;
	newBlock->pre.pointer = newBlock;
	newBlock->index = newBlock->index + 1;
	newBlock->endSmallPos = 0;
	return newBlock;
}