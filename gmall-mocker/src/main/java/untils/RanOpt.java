package untils;
//加权
public class RanOpt<T> {

    private T value;  //值
    private int weight;  //权重

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
