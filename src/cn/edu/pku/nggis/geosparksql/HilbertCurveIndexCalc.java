package cn.edu.pku.nggis.geosparksql;

public abstract class HilbertCurveIndexCalc {

	private static int MAX_INPUT_VAL = 0x4000000;

	public static long xy2d(double xd, double yd) {
		if (xd >= 1 || xd < 0 || yd >= 1 || yd < 0)
			throw new IllegalArgumentException(String.format(
					"value of x:%f and y:%f must between [0,1)", xd, yd));
		int x = (int) (xd * MAX_INPUT_VAL), y = (int) (yd * MAX_INPUT_VAL);
		int rx, ry, s;
		long d = 0;
		for (s = MAX_INPUT_VAL / 2; s > 0; s /= 2) {
			rx = (x & s) > 0 ? 1 : 0;
			ry = (y & s) > 0 ? 1 : 0;
			d += s * s * ((3 * rx) ^ ry);
			if (ry == 0) {
				if (rx == 1) {
					x = MAX_INPUT_VAL - 1 - x;
					y = MAX_INPUT_VAL - 1 - y;
				}

				// Swap x and y
				int t = x;
				x = y;
				y = t;
			}
		}
		return d;
	}

	public static void main(String[] args) {
		System.out.println(xy2d(4.0 / MAX_INPUT_VAL, 1.0 / MAX_INPUT_VAL));
	}

}
