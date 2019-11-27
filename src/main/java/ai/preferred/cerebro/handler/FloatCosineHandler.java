package ai.preferred.cerebro.handler;

/**
 * Child class of {@link VecFloatHandler} with detailed implementation of
 * the distance function using cosine metric
 *
 * Whenever you need to use a new metric just create a new child class like this
 *
 * @author hpminh@apcs.vn
 */
public final class FloatCosineHandler extends VecFloatHandler {
    @Override
    public double distance(float[] a, float[] b) {
        float dot = 0.0f;
        float nru = 0.0f;
        float nrv = 0.0f;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            nru += a[i] * a[i];
            nrv += b[i] * b[i];
        }

        float similarity = dot / (float)(Math.sqrt(nru) * Math.sqrt(nrv));
        return 1 - similarity;
    }
}
