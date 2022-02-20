/**
 * Clasa Worker care implementeaza Runnable si executa o operatie de map
 * sau de reduce (in functie de parametrul primit in constructor).
 */
public class Worker implements Runnable {
    private Mapper mapTask = null;
    private Reducer reduceTask = null;

    public Worker(MapReduceInterface task, OPERATION_TYPE type) {
        if (type == OPERATION_TYPE.MAP)
            this.mapTask = (Mapper) task;
        else
            this.reduceTask = (Reducer) task;
    }
    @Override
    public void run() {
        if (mapTask != null)
            mapTask.map();
        else
            reduceTask.reduce();
    }
}
