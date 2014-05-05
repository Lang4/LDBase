#pragma once
/**
 * ���ݳ־û����� Author jijinlong 
 * email jjl_2009_hi@163.com
 */
#include <stdio.h>
#include <string.h>

#define IDLES_BLOCK_SIZE 1024

#pragma pack(1)
/**
 * ƫ��ֵ
 */
class LDPos{
public:
	unsigned long value; // �ļ��е�λ��
	void *pointer; // ������ڴ��
	LDPos(unsigned long value)
	{
		this->value = value;
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
 * С��
 **/
class LDSmallBlock{
public:
	enum{
		STREAM = 0,
		BASE = 1,
	};
	short index; // ���
	void* value; // ��ֵ
	short tag; // ֵ����
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
 * ���
 */
class LDBigBlock{
public:
	LDPos pre; // ǰһ����
	unsigned long endSmallPos; // ��ǰ���β
	unsigned long offset; // ƫ��
	unsigned long index; // ��ǰ�������
	LDSmallBlock blocks[1024]; // ÿ�����1024 ��С��
	LDPos next; // ��һ����
	char idleSpace[2]; // ���б�ʾ
	LDBigBlock()
	{
		memset(this,0,sizeof(*this));
		offset = 0;
	}
	/**
	 * ��ȡС��
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
	 * ��鵱ǰTag ֵ
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
			b->index = tag % IDLES_BLOCK_SIZE;
			b->tag = LDSmallBlock::BASE;
		}
	}
	unsigned long getNowPos();
};

/**
 * ���д���ʾ
 **/
class LDBigIdlesBlock{
public:
	char value[IDLES_BLOCK_SIZE]; // ��ʾ���Ŀ�����Ϣ 1λ��Ӧһ�� һ����ʾ�� ֻ�� 1024 * 8 �Ĵ��
	LDPos next; // ��һ����ʾ��
	LDPos pre; // ��һ����ʾ��
	LDPos now;// ��ǰλ��
	char hadIdle; // �Ƿ��п���
	LDBigIdlesBlock()
	{
		memset(this,0,sizeof(*this));
	}
	unsigned long  getNowPos();
};
/**
 * ��ǰд������¼
 **/
class LDBWriteSingal{
public:
	LDBWriteSingal(){
		tag = 0;
	}
	LDPos bigBlock; // ��ǰ�����Ϣ
	LDPos oldBigBlock; // ��һ�εĴ��
	LDSmallBlock oldSmallBlock; // ���µ�С�鱸��
	int tag; // ��ǰ������ʾ
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
	char tag[2]; // ��ʾ
	LDBigIdlesBlock idleBlockHead; // ������ʼ��
	LDBigBlock bigBlock; // ��ʼ���
	LDBWriteSingal signal; // д����Ϣ��
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
	unsigned long getWriteSignalPos()
	{
		return sizeof(tag) + sizeof(idleBlockHead) + sizeof(bigBlock);
	}
	void clearSingal()
	{
		memset(&signal,0,sizeof(signal));
	}
};
#pragma pack()

/**
 * �ļ����� ����������ȷд��
 */
class LDBFile{
public:
	LDBFile(const char *fileName);
	/**
	 * ��ʽ��
	 */
	bool format();
	/**
	 * �Ƿ������ݿ��ļ�
	 */
	bool isDB();
	/**
	 * ����ͷ
	 */
	void  readHead();

	/**
	 * ���ļ�
	 */
	bool open(bool withFormat = false);


	/**
	 * д���ļ���
	 */
	bool writeBlock(void *content,unsigned int len);

	/**
	 * ��ȡ�ļ���
	 */
	bool readBlock(void *content,unsigned int len);

	/**
	 * �ƶ���ĳ����
	 */
	void moveTo(const unsigned long &pos);

	~LDBFile();
	/**
	 * DB ͷ 
	 */
	LDBHead head;
	/**
	 * ��ȡ����λ��
	 */
	LDPos getIdlePos();
	/**
	 * ���ÿ���λ��
	 */
	void resetIdle(unsigned long index);
	/**
	 * ��ȡ����λ��
	 */
	char* getIdleChar(unsigned long index);
	/**
	 * ���ÿ���λ��
	 */
	void setIdle(unsigned long  index);
	/**
	 * ������һ������λ��
	 */
	LDBigIdlesBlock * newNextIdleBlock(LDBigIdlesBlock *nowBlock);
	/**
	 * ����д�ź�
	 */
	void updateWirteSignal();
	/**
	 * д����
	 */
	void writeIdles();
private:
	FILE *handle;
};

/**
 * �ļ����� 
 * ����顢���տ�
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
	}
	LDBStream(LDBStream &stream)
	{
		 
	}
	/**
	 * ���ļ�
	 */
	bool open(bool withFormat = false)
	{
		if (!_file) return false;
		if (_file->open(withFormat))
		{
			head = &_file->head.bigBlock;
			return true;
		}
		return false;
	}
	/**
	 * д���������
	 */
	template<typename type>
	void write_base(type a,unsigned long tag)
	{
		LDBigBlock *nowBlock = head;
		LDBigBlock *pre = head;
		// ��ȡ�����ӿ�
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
			nowBlock->write(a,tag);
		}
	}
	/**
	 * д�����
	 */
	template<typename object>
	void write(object& o,unsigned long tag)
	{
		LDBStream ss;
		ss.head = newNextBlock(head);
		ss.setTag(OUT);
		ss & a;
		write(ss,tag);
	}
	/**
	 * д��������
	 **/
	void write(LDBStream& ss,unsigned long tag);
	/**
	 * ���ݵ�ǰ���ȡ��һ����
	 * ���������ڴ��У��������ڴ������Ƿ����ļ���,�����ļ�����������λ��
	 */
	LDBigBlock * getNext(LDBigBlock * block);
	/**
	 * ������һ����
	 */
	LDBigBlock * newNextBlock(LDBigBlock *nowBlock);
	LDBigBlock *head; // ��ǰ��ͷ
	/**
	 * ����Ϊtag������
	 */
	void save(int tag);

	/**
	 * д����
	 * 
	 */
	void writeBigBlock(LDBigBlock *block);
	
	/**
	 * д��С��
	 */
	void writeSmallBlock(LDSmallBlock *small,const unsigned long &dest);
private:
	LDBFile *_file;
};