package buaa.act.ucar.datasimu.config;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class KFKPartitioner implements Partitioner {
	// 构造函数，没有的话会报错
	public KFKPartitioner(VerifiableProperties prop) {
		// TODO 自动生成的构造函数存根
	}

	public int partition(Object key, int numPar) {
		if (key == null) {
			Random random = new Random();
			return random.nextInt(numPar);
		} else {
			int result = Math.abs(((String) key).hashCode()) % numPar;
			return result;
		}
	}
}
