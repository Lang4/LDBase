#include "LDBase.h"
/**
 * LDBStream ��Ҫ֧�ֵĹ�����
 * 1: ����
 * 2: �ֲ�����
 * 3: ����ʱ�̹��� �ɻָ�
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