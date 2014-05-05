#pragma once
/**
 * 数据持久化方案 Author jijinlong 
 * email jjl_2009_hi@163.com
 */
#include <stdio.h>
#include <string.h>
#pragma pack(1)
/**
 * 偏移值
 */
class LDPos{
public:
	unsigned long value; // 文件中的位置
	void *pointer; // 对象的内存块
	LDPos(unsigned long value)
	{
		value = value;
		pointer = NULL;
	}
	LDPos()
	{
		memset(this,0,sizeof(*this));
	}
	LDPos & operator=(const LDPos &pos)
	{
		this->value = pos.value;
		this->pointer = pos.pointer;
		return *this;
	}
};
/**
 * 小块
 **/
class LDSmallBlock{
public:
	enum{
		STREAM = 0,
	};
	short index; // 编号
	void* value; // 数值
	short tag; // 值属性
	LDSmallBlock()
	{
		memset(this,0,sizeof(*this));
	}
	template<typename object>
	void write(object& o)
	{
		value = (void*)o;
	}
	unsigned long getValue()
	{
		return (unsigned long) value;
	}
	unsigned long getNowPos()
	{
		return index * sizeof(LDSmallBlock);
	}
	bool equal(const LDSmallBlock &block)
	{
		if (index == block.index && value == block.value && tag == block.tag)
			return true;
		return false;
	}
};
/**
 * 大块
 */
class LDBigBlock{
public:
	LDPos pre; // 前一个块
	unsigned long endSmallPos; // 当前块结尾
	unsigned long offset; // 偏移
	unsigned long index; // 当前引索编号
	LDSmallBlock blocks[1024]; // 每个大块1024 个小块
	LDPos next; // 后一个块
	char idleSpace[2]; // 空闲标示
	LDBigBlock()
	{
		memset(this,0,sizeof(*this));
		offset = 0;
	}
	/**
	 * 获取小块
	 */
	LDSmallBlock * getSmallBlock(unsigned long tag)
	{
		if (checkTag(tag))
		{
			return & blocks[tag - offset];
		}
		return NULL;
	}
	/**
	 * 检查当前Tag 值
	 */
	bool checkTag(unsigned long tag)
	{
		if (tag >= offset && tag < offset + 1024)
		{
			return true;
		}
		return false;
	}
	template<typename object>
	void write(object& o,unsigned long tag)
	{
		LDSmallBlock *b = getSmallBlock(tag);
		if (b)
		{
			b->write(o);
		}
	}
	LDPos getNowPos();
};
#define IDLES_BLOCK_SIZE 1024
/**
 * 空闲大块标示
 **/
class LDBigIdlesBlock{
public:
	char value[IDLES_BLOCK_SIZE]; // 标示大块的空闲信息 1位对应一块 一个标示块 只是 1024 * 8 的大块
	LDPos next; // 下一个标示块
	LDPos pre; // 上一个标示块
	LDPos now;// 当前位置
	char hadIdle; // 是否还有空闲
	LDBigIdlesBlock()
	{
		memset(this,0,sizeof(*this));
	}
	LDPos getNowPos();
};
/**
 * 当前写操作记录
 **/
class LDBWriteSingal{
public:
	LDBWriteSingal(){
		tag = 0;
	}
	LDPos bigBlock; // 当前大块信息
	LDPos idleBlock; // 当前空闲块信息
	LDPos oldBigBlock; // 上一次的块
	LDSmallBlock oldSmallBlock; // 更新的小块备份
	int tag; // 当前操作标示
	~LDBWriteSingal(){}
	enum{
		WRITE = 0,
		WRITE_IDLE = 1,
		WRITE_BIG = 2,
		WRITE_SMALL = 3,
	};
	void start(int w)
	{
		tag |= 1 << w; 
	}
	void close(int w)
	{
		tag &= ~(1 << w);
	}
	void clear()
	{
		tag = 0;
	}
	bool isDirty()
	{
		return tag != 0;
	}
};

struct LDBHead{
	char tag[2]; // 标示
	LDBigIdlesBlock idleBlockHead; // 空闲起始块
	LDBigBlock bigBlock; // 起始大块
	LDBWriteSingal signal; // 写入信息池
	char version[4]; // "ldb1"
	LDBHead()
	{
		reset();
	}
	void reset()
	{
		clear();
		tag[0] = 'L';
		tag[1] = 'D';
		version[0] = 'L';
		version[1] = 'D';
		version[2] = 'B';
		version[3] = '1';
	}
	void clear()
	{
		memset(this,0,sizeof(*this));
	}
	LDPos getWriteSignalPos()
	{
		return sizeof(tag) + sizeof(idleBlockHead) + sizeof(bigBlock);
	}
};
#pragma pack()

/**
 * 文件操作 控制流的正确写入
 */
class LDBFile{
public:
	LDBFile(const char *fileName);
	/**
	 * 格式化
	 */
	bool format();
	/**
	 * 是否是数据库文件
	 */
	bool isDB();
	/**
	 * 读入头
	 */
	void  readHead();

	/**
	 * 打开文件
	 */
	bool open(bool withFormat = false);


	/**
	 * 写入文件块
	 */
	bool writeBlock(void *content,unsigned int len);

	/**
	 * 读取文件块
	 */
	bool readBlock(void *content,unsigned int len);


	/**
	 * 移动到某个点
	 */
	void moveTo(const LDPos &pos);

	~LDBFile();

	LDBHead head;
	/**
	 * 获取空闲位置
	 */
	LDPos getIdlePos()
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
							unsigned long pos = (i * 8 + j) + sizeof(LDBHead);
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
	LDBigIdlesBlock * newNextIdleBlock(LDBigIdlesBlock *nowBlock)
	{
		LDBigIdlesBlock *newBlock = new LDBigIdlesBlock;
		nowBlock->next = newBlock->now;
		newBlock->pre = nowBlock->now;
		newBlock->now.value = nowBlock->now.value + 1;
		return newBlock;
	}
	void updateWirteSignal()
	{
		if (handle)
		{
			moveTo(head.getWriteSignalPos());
			writeBlock(&head.signal,sizeof(head.signal));
		}
	}
private:
	FILE *handle;
};

/**
 * 文件操作 
 * 声请块、回收块
 */
class LDBStream{
public:
	LDBStream()
	{
		_file = NULL;
		head = NULL;
	}
	LDBStream(const char *fileName)
	{
		_file = new LDBFile(fileName);
		head = &_file->head.bigBlock;
	}
	LDBStream(LDBStream &stream)
	{
		 
	}
	/**
	 * 打开文件
	 */
	bool open(bool withFormat = false)
	{
		if (!_file) return false;
		return _file->open(withFormat);
	}
	template<typename type>
	void write_base(type a,unsigned long tag)
	{
		LDBigBlock *nowBlock = head;
		// 获取空闲子块
		while (nowBlock && !nowBlock->checkTag(tag))
		{
			nowBlock = getNext(nowBlock);
		}
		if (!nowBlock)
		{
			nowBlock = newNextBlock(nowBlock);
			nowBlock->offset = tag / IDLES_BLOCK_SIZE;
		}
		if (nowBlock)
			nowBlock->write(a,tag);
	}
	
	template<typename object>
	void write(object& o,unsigned long tag)
	{
		LDBStream ss;
		ss.head = newNextBlock(head);
		ss.setTag(OUT);
		ss & a;
		write(ss,tag);
	}
	void write(LDBStream& ss,unsigned long tag);
	/**
	 * 根据当前块获取下一个块
	 * 可能是在内存中，若不在内存中则看是否在文件中,不在文件中则分配空闲位置
	 */
	LDBigBlock * getNext(LDBigBlock * block);
	/**
	 * 创建下一个块
	 */
	LDBigBlock * newNextBlock(LDBigBlock *nowBlock);
	LDBigBlock *head; // 当前块头

	void save(int tag);

	/**
	 * 写入大块
	 * 
	 */
	void writeBigBlock(LDBigBlock *block)
	{
		if (!_file) return;
		LDBigBlock *nowBlock = block;
		while (nowBlock)
		{
			_file->head.signal.start(LDBWriteSingal::WRITE_BIG);
			_file->head.signal.oldBigBlock = nowBlock->getNowPos(); 
			_file->head.signal.bigBlock = nowBlock->getNowPos(); 
			_file->updateWirteSignal(); // 先写操作信号
			_file->moveTo(nowBlock->getNowPos());
			_file->writeBlock(nowBlock,sizeof(LDBigBlock));
			/**
			 * 回收老块
			 */
			/**
			 * 写空闲块
			 */
			/**
			 * 写大块
			 */
			_file->head.signal.close(LDBWriteSingal::WRITE_BIG);
			_file->updateWirteSignal(); // 关闭写操作信号
			nowBlock = (LDBigBlock *)nowBlock->next.pointer;
		}
	}
	/**
	 * 写入小块
	 */
	void writeSmallBlock(LDSmallBlock *small)
	{
		/**
		 * 先备份
		 */
		_file->head.signal.start(LDBWriteSingal::WRITE_SMALL); // 当前写操作
		_file->moveTo(small->getNowPos());
		_file->readBlock(&_file->head.signal.oldSmallBlock,sizeof(_file->head.signal.oldSmallBlock));// 将老的小块读出
		if (_file->head.signal.oldSmallBlock.equal(*small))
		{
			_file->head.signal.close(LDBWriteSingal::WRITE_SMALL); // 关闭写小块操作
			return;
		}
		_file->updateWirteSignal(); // 更新信号到文件
		_file->moveTo(small->getNowPos());
		_file->writeBlock(small,sizeof(LDSmallBlock)); // 更新小块
	}
private:
	LDBFile *_file;
};