#include "LDBase.h"
#include <io.h>
unsigned long LDBigIdlesBlock::getNowPos()
{
	unsigned long value = 2;
	if (now.value) value = sizeof(LDBHead) + (now.value - 1)* (sizeof(LDBigIdlesBlock) + sizeof(LDBigBlock));
	return (value);
}
unsigned long LDBigBlock::getNowPos()
{
	unsigned long value = 2 + sizeof(LDBigIdlesBlock);
	if (index) value = sizeof(LDBHead) + (index -1) * (sizeof(LDBigIdlesBlock) + sizeof(LDBigBlock)) + sizeof(LDBigIdlesBlock);
	return (value);
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
	moveTo(0);
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
	moveTo(0);
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
 * д���ļ���
 */
bool LDBFile::writeBlock(void *content,unsigned int len)
{
	if (!handle) return false;
	fwrite(content,len,1,handle);
	return true;
}

/**
 * ��ȡ�ļ���
 */
bool LDBFile::readBlock(void *content,unsigned int len)
{
	if (!handle) return false;
	fread(content,len,1,handle);
	return true;
}


/**
 * �ƶ���ĳ����
 */
void LDBFile::moveTo(const unsigned long &pos)
{
	if (!handle) return;
	fseek(handle,pos,SEEK_SET);
}

LDPos LDBFile::getIdlePos()
{
	LDBigIdlesBlock *nowBlock = &head.idleBlockHead;
	while (nowBlock)
	{
		if (nowBlock->hadIdle)
		{
			for ( int i = 0; i < sizeof(nowBlock->value);i++)
			{
				char & tag = nowBlock->value[i];
				for (int j = 0;j < 8;j++)
				{
					if (0 == (tag & (1 << j)))
					{
						unsigned long pos = (i * 8 + j);
						return LDPos(pos);
					}
				}
			}
		}
		nowBlock->hadIdle = false;
		if (nowBlock->next.pointer)
		{
			nowBlock = (LDBigIdlesBlock *)nowBlock->next.pointer;
			continue;
		}
		if (nowBlock->next.value != 0)
		{
			moveTo(nowBlock->getNowPos());
			nowBlock = newNextIdleBlock(nowBlock);
			readBlock(nowBlock,sizeof(LDBigIdlesBlock));
			continue;
		}
		nowBlock = newNextIdleBlock(nowBlock);
	}
	return LDPos(0);
}
void LDBFile::resetIdle(unsigned long index)
{
	char *p = getIdleChar(index);
	if (p)
	{
		(*p) &= ~ (1 << (index % 8));
	}
}
char* LDBFile::getIdleChar(unsigned long index)
{
	LDBigIdlesBlock *nowBlock = &head.idleBlockHead;
	unsigned long offset = index / (8 * IDLES_BLOCK_SIZE);
	while (nowBlock && offset -- >= 0)
	{
		if (nowBlock->next.pointer)
		{
			nowBlock = (LDBigIdlesBlock *)nowBlock->next.pointer;
			continue;
		}
		if (nowBlock->next.value != 0)
		{
			moveTo(nowBlock->getNowPos());
			nowBlock = newNextIdleBlock(nowBlock);
			readBlock(nowBlock,sizeof(LDBigIdlesBlock));
			continue;
		}
		nowBlock = newNextIdleBlock(nowBlock);
	}
	if (nowBlock)
	{
		offset = index % (8 * IDLES_BLOCK_SIZE);
		return & nowBlock->value[offset];
	}
	return NULL;
}
void LDBFile::setIdle(unsigned long  index)
{
	char *p = getIdleChar(index);
	if (p)
	{
		(*p) |= (1 << (index % 8));
	}
}
LDBigIdlesBlock * LDBFile::newNextIdleBlock(LDBigIdlesBlock *nowBlock)
{
	LDBigIdlesBlock *newBlock = new LDBigIdlesBlock;
	nowBlock->next = newBlock->now;
	newBlock->pre = nowBlock->now;
	newBlock->now.value = nowBlock->now.value + 1;
	return newBlock;
}
void LDBFile::updateWirteSignal()
{
	if (handle)
	{
		moveTo(head.getWriteSignalPos());
		writeBlock(&head.signal,sizeof(head.signal));
	}
}
void LDBFile::writeIdles()
{
	head.signal.start(LDBWriteSingal::WRITE_IDLE);
	updateWirteSignal();
	
	LDBigIdlesBlock *nowBlock = &head.idleBlockHead;
	while (nowBlock)
	{
		this->moveTo(nowBlock->getNowPos());
		this->writeBlock(nowBlock,sizeof(LDBigIdlesBlock));
		if (nowBlock->next.pointer)
		{
			nowBlock = (LDBigIdlesBlock *)nowBlock->next.pointer;
		}
		else
			nowBlock = NULL;
	}
	
	head.signal.close(LDBWriteSingal::WRITE_IDLE);
	updateWirteSignal();
}

LDBFile::~LDBFile()
{
	if (handle)
		fclose(handle);
}


void LDBStream::write(LDBStream& ss,unsigned long tag)
{
	// ��ȡ�����ӿ�
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
			writeSmallBlock(small,(nowBlock->getNowPos() + small->getNowPos())); // д��С��
			writeBigBlock(bigBlock); // д����
		}
		else
		{
			unsigned long pos = nowBlock->getNowPos();
			pos += small->getNowPos();
			writeSmallBlock(small,(pos)); // д��С��
		}
	}
	_file->head.clearSingal();
	_file->updateWirteSignal();
}

/**
 * ������һ����
 */
LDBigBlock * LDBStream::newNextBlock(LDBigBlock *nowBlock)
{
	if (nowBlock->next.pointer) return (LDBigBlock*) nowBlock->next.pointer;
	LDBigBlock *newBlock = new LDBigBlock;
	nowBlock->next.pointer = newBlock;
	newBlock->pre.pointer = newBlock;
	newBlock->index = 0;
	newBlock->idleSpace[0] = '1';
	newBlock->endSmallPos = 0;
	return newBlock;
}

/**
 * д����
 * 
 */
void LDBStream::writeBigBlock(LDBigBlock *block)
{
	if (!_file) return;
	LDBigBlock *nowBlock = block;
	while (nowBlock)
	{
		_file->head.signal.start(LDBWriteSingal::WRITE_BIG);
		_file->head.signal.oldBigBlock = nowBlock->getNowPos(); 
		nowBlock->index = _file->getIdlePos().value;
		_file->head.signal.bigBlock = nowBlock->getNowPos(); 
		_file->updateWirteSignal(); // ��д�����ź�
		_file->moveTo(nowBlock->getNowPos());
		/**
		 * д���
		 */
		_file->writeBlock(nowBlock,sizeof(LDBigBlock));
		/**
		 * �����Ͽ�
		 */
		_file->resetIdle(_file->head.signal.oldBigBlock.value);
		/**
		 * д���п�
		 */
		_file->setIdle(nowBlock->index);
		
		_file->head.signal.close(LDBWriteSingal::WRITE_BIG);
		_file->updateWirteSignal(); // �ر�д�����ź�
		nowBlock = (LDBigBlock *)nowBlock->next.pointer;
	}
	_file->writeIdles(); // ���ļ�ϵͳ����
}
/**
 * д��С��
 */
void LDBStream::writeSmallBlock(LDSmallBlock *small,const unsigned long &dest)
{
	/**
	 * �ȱ���
	 */
	_file->head.signal.start(LDBWriteSingal::WRITE_SMALL); // ��ǰд����
	_file->moveTo(small->getNowPos());
	_file->readBlock(&_file->head.signal.oldSmallBlock,sizeof(_file->head.signal.oldSmallBlock));// ���ϵ�С�����
	if (_file->head.signal.oldSmallBlock.equal(*small))
	{
		_file->head.signal.close(LDBWriteSingal::WRITE_SMALL); // �ر�дС�����
		return;
	}
	_file->updateWirteSignal(); // �����źŵ��ļ�
	_file->moveTo(dest);
	_file->writeBlock(small,sizeof(LDSmallBlock)); // ����С��
}