package edu.cmu.graphchi.datablocks;

/**
 * @author Aapo Kyrola
 */
public class BooleanConverter implements BytesToValueConverter<Boolean> {
    @Override
    public int sizeOf() {
        return 1;
    }

    @Override
    public Boolean getValue(byte[] array) {
        return array[0] == 1;
    }

    @Override
    public void setValue(byte[] array, Boolean val) {
        array[0] = (byte) (val ? 1 : 0);
    }
}
