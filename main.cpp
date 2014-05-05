#include "LDBase.h"
/**
 * LDBStream 主要支持的功能有
 * 1: 备份
 * 2: 局部更新
 * 3: 任意时刻故障 可恢复
 **/
int main()
{
	LDBStream stream("tr.ser");
	if (stream.open(true))
	{
		int value = 199;
		stream.write_base(value,1);
		stream.save(1);
	}
}