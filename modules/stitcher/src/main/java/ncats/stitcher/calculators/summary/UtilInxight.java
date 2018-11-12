package ncats.stitcher.calculators.summary;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UtilInxight {
    private static Pattern URI_REGEX = Pattern.compile("http[s]*:[/][/][^ ]*");

    /**
     * Extract all URLs from text, and place them in a list. Returns a {@link Tuple}
     * of both the modified text as well as the list of URLs. That list will be empty
     * if no URLs are found.
     *
     * @param text
     * @return
     */
    public static Tuple<String,List<String>> extractUrls(String text){

        Matcher m = URI_REGEX.matcher(text);

        List<String> urls = new ArrayList<String>();

        while(m.find()){
            urls.add(text.substring(m.start(),m.end()));
        }
        if(!urls.isEmpty()){
            m.reset(text);
            text=m.replaceAll("");
        }

        return Tuple.of(text, urls);

    }

    private static class CounterFunction<K> implements Function<K,Tuple<Integer,K>>{
        private final AtomicInteger count;

        public CounterFunction(){
            this(0);
        }

        public CounterFunction(int initialValue){
            count = new AtomicInteger(initialValue);
        }
        @Override
        public Tuple<Integer, K> apply(K k) {

            return new Tuple<Integer,K>(count.getAndIncrement(),k);
        }
    }

    public static <K> Function<K,Tuple<Integer,K>> toIndexedTuple(){
        return new CounterFunction<K>();
    }
    public static <K> Function<K,Tuple<Integer,K>> toIndexedTuple(int initialValue){
        return new CounterFunction<K>(initialValue);
    }


    public static <T> Comparator<T> comparatorItems(T ... order){
        return comparator(t->t, Arrays.stream(order));
    }
    public static <T> Comparator<T> comparator(Stream<T> order){
        return comparator(t->t, order);
    }

    public static <T> Comparator<T> comparator(Collection<T> order){
        return comparator(t->t, order.stream());
    }

    public static <T> Comparator<T> comparator(T[] order){
        return comparator(t->t, Arrays.stream(order));
    }

    public static <T, V> Comparator<V> comparator(Function<V,T> namer, Stream<T> order){
        Map<T,Integer> mapOrder=order.map(toIndexedTuple())
                .collect(Collectors.toMap(t->t.v(),
                        t->t.k(),
                        (a,b)->a)); //Keep old values
        return (a,b)->{
            T k1=namer.apply(a);
            T k2=namer.apply(b);
            Integer i1=mapOrder.getOrDefault(k1, Integer.MAX_VALUE);
            Integer i2=mapOrder.getOrDefault(k2, Integer.MAX_VALUE);
            return Integer.compare(i1, i2);
        };
    }

    public static class Final<T>{
        private T t;
        public T get(){
            return t;
        }

        public T getAndNullify(){
            T t=get();
            set(null);
            return t;
        }

        public void set(T t){
            this.t=t;
        }

        public static<T>  Final<T> of(T t){
            Final<T> f=new Final<T>();
            f.set(t);
            return f;
        }
    }

}
