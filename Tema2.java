import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/** *
 * Enum folosit pentru functia compute pentru a stii worker-ul ce tip de task
 * primeste.
 */
enum OPERATION_TYPE {
  MAP,
  REDUCE
}

public class Tema2 {
    private static int P;
    private static int D;
    private static final ArrayList<String> files = new ArrayList<>();
    private static final HashMap<String,
            ArrayList<Pair<HashMap<Integer, Integer>, ArrayList<String>>>>
            map = new HashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        P = Integer.parseInt(args[0]);
        read_info(args[1]);
        var mapTasks = create_map_tasks();
        compute(mapTasks, OPERATION_TYPE.MAP);
        var reduceTasks = combine(mapTasks);
        compute(reduceTasks, OPERATION_TYPE.REDUCE);
        sortAndPrint(reduceTasks, args[2]);
    }

    /**
     * Metoda auxiliara folosita pentru a printa rezultatele in ordine
     * descrescatoare.
     * @param reduceTasks lista de taskuri de tip reduce care au fost in
     *                    prealabil rezolvate.
     * @param output numele fisierului in care se vor scrie rezultatele
     */
    private static void sortAndPrint(ArrayList<Reducer> reduceTasks,
                                     String output) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter(output));
        while (reduceTasks.size() > 0) {
            float max = -1;
            int index = -1;
            for (int i = 0; i < reduceTasks.size(); ++i) {
                if (reduceTasks.get(i).rank > max) {
                    max = reduceTasks.get(i).rank;
                    index = i;
                }
            }
            writer.write(reduceTasks.get(index).result + "\n");
            reduceTasks.remove(index);
        }
        writer.close();
    }

    /**
     * Metoda care combina rezultatele taskurilor de map si pregateste
     * inputul pentru taskurile de reduce.
     * @param mapTasks lista de taskuri de tip map care au fost rezolvate
     * @return lista de taskuri de tip reduce
     */
    private static ArrayList<Reducer> combine(ArrayList<Mapper> mapTasks) {
        for (var mapTask : mapTasks) {
            map.putIfAbsent(mapTask.file_name, new ArrayList<>());
            var value = map.get(mapTask.file_name);
            value.add(mapTask.result);
        }
        ArrayList<Reducer> reduceTasks = new ArrayList<>();
        for (var iterator : map.entrySet())
            reduceTasks.add(new Reducer(iterator.getKey(), iterator.getValue()));
        return reduceTasks;
    }

    /**
     * Metoda generica care primeste o lista de taskuri si tipul taskurilor.
     * Creaza thread-uri cat timp mai exista taskuri nerezolvate.
     * @param tasks lista de taskuri
     * @param type tipul taskului
     * @param <T> tipul generic (acesta trebuie sa extinda MapReduceInterface
     *           (adica sa fie ori Mapper ori Reducer)
     */
    private static <T extends MapReduceInterface> void
        compute(ArrayList<T> tasks, OPERATION_TYPE type) throws InterruptedException {
        Thread[] threads = new Thread[P];
        int noTasksDone = 0;
        while (noTasksDone < tasks.size()) {
            int workersNo = 0;
            for (int i = 0; i < P; ++i, ++workersNo) {
                if (noTasksDone == tasks.size())
                    break;
                threads[i] = new Thread(new Worker(tasks.get(noTasksDone), type));
                threads[i].start();
                noTasksDone++;
            }
            for (int i = 0; i < workersNo; ++i) {
                threads[i].join();
            }
        }
    }

    /**
     * Metoda care creaza taskurile de map impartind dimensiunea
     * fisierelor primite ca input.
     * @return lista de taskuri de map (Mapper)
     */
    private static ArrayList<Mapper> create_map_tasks() {
        ArrayList<Mapper> mapTasks = new ArrayList<>();
        for (var file : files) {
            Path path = Paths.get(file);
            long bytes = 0;
            try {
                bytes = Files.size(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
            int offset = 0;
            while (bytes > D) {
                bytes -= D;
                Mapper mapper = new Mapper(file, D, offset);
                mapTasks.add(mapper);
                offset += D;
            }
            if (bytes > 0) {
                Mapper mapper = new Mapper(file, (int) bytes, offset);
                mapTasks.add(mapper);
            }
        }
        return mapTasks;
    }

    /**
     * Metoda care citeste inputul.
     * @param in_file fisierul de input
     */
    private static void read_info(String in_file) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(in_file));
        D = scanner.nextInt();
        int n = scanner.nextInt();
        scanner.nextLine();
        for (int i = 0; i < n; ++i)
            files.add(scanner.nextLine());
        scanner.close();
    }
}
