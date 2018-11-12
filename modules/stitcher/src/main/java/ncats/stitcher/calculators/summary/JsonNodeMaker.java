package ncats.stitcher.calculators.summary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.fge.jsonpatch.JsonPatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by katzelda on 8/1/17.
 */
public class JsonNodeMaker {
	private Map<String, String> props;
	private List<Tuple<String, String>> grab = new ArrayList<>();
	private Map<String, Function<String, JsonNode>> jsonParsers = new HashMap<>();

	JsonNode orig;
	
	
	private ObjectMapper om = new ObjectMapper();

	public static JsonNodeMaker fromJson(String json){
		try {
			return new JsonNodeMaker(new ObjectMapper().readTree(json));
		}catch(IOException e){
			throw new UncheckedIOException(e);
		}
	}

	public JsonNodeMaker(JsonNode original) {
		this(new ObjectMapper().convertValue(original, Map.class));
		this.orig=original;
	}
	public JsonNodeMaker(JsonNode original, String...keysToKeep) {
		this(new ObjectMapper().convertValue(original, Map.class));
		this.orig=original;
		
		Set<String> keep = new HashSet<>();
		for(String k : keysToKeep){
			keep.add(k);
		}

		Iterator<Map.Entry<String,String>> iter =  props.entrySet().iterator();
		while(iter.hasNext()){
			if(!keep.contains(iter.next().getKey())){
				iter.remove();
			}
		}
	}
	public JsonNodeMaker(Map<String, String> op) {
		this.props = op;
	}

	public JsonNodeMaker add(String oprop, String nprop) {
		this.grab.add(Tuple.of(nprop, oprop));
		return this;
	}

	public JsonNodeMaker addList(String oprop, String nprop, String delim) {
		this.grab.add(Tuple.of(nprop, oprop));
		this.jsonParsers.put(nprop, (s) -> {

			ArrayNode an = om.createArrayNode();

			Arrays.stream(s.split(delim))
					.forEach(v -> {
						an.add(v);
					});

			return an;
		});
		return this;
	}
	
	public JsonNodeMaker addNativeList(String oprop, String nprop, String delim) {
		this.grab.add(Tuple.of(nprop, oprop));
		this.jsonParsers.put(nprop, (s) -> {

			ArrayNode an = om.createArrayNode();

			Arrays.stream(s.split(delim))
					.forEach(v -> {
						an.add(v);
					});

			return an;
		});
		return this;
	}


	public JsonNodeMaker addExplicit(String oprop, String nprop) {
		String rando = UUID.randomUUID().toString();
		this.grab.add(Tuple.of(nprop, rando));
		this.props.put(rando, oprop);
		return this;
	}
	
	
	private BiFunction<JsonNode,JsonNode,JsonNode> runAfter = (j1, j2)->j2;
	
	
	public JsonNodeMaker move(String oldPath, String newPath){
		String op;
		if(!oldPath.startsWith("/")){
			op="/"+oldPath;
		}else{
			op=oldPath;
		}
		
		String np;
		if(!newPath.startsWith("/")){
			np="/"+newPath;
		}else{
			np=newPath;
		}
		
		
		BiFunction<JsonNode,JsonNode,JsonNode> runAfterBefore=this.runAfter;
		runAfter= (j1,j2)->{
				JsonNode jnew=runAfterBefore.apply(j1, j2);
				JsonNode oldval=j1.at(op);
				
				ObjectNode on = om.createObjectNode();
				
				on.put("path", np);
				on.put("op", "add");
				on.put("value", oldval);
				
				try {
					JsonPatch jp=JsonPatch.fromJson(on);
					return jp.apply(jnew);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return on;
		};
		return this;
	}
	
	public JsonNodeMaker moveToArray(String oldPath, String newPath){
		String op;
		if(!oldPath.startsWith("/")){
			op="/"+oldPath;
		}else{
			op=oldPath;
		}
		
		String np;
		if(!newPath.startsWith("/")){
			np="/"+newPath;
		}else{
			np=newPath;
		}
		
		
		BiFunction<JsonNode,JsonNode,JsonNode> runAfterBefore=this.runAfter;
		runAfter= (j1,j2)->{
				JsonNode jnew=runAfterBefore.apply(j1, j2);
				JsonNode oldval=j1.at(op);
				
				if(!oldval.isArray()){
					ArrayNode an = om.createArrayNode();
					an.add(oldval);
					oldval=an;
				}
				
				ArrayNode anpatch = om.createArrayNode();
				
				
				ObjectNode on = om.createObjectNode();
				
				on.put("path", np);
				on.put("op", "add");
				on.put("value", oldval);
				anpatch.add(on);
				try {
					
					JsonPatch jp=JsonPatch.fromJson(anpatch);
					return jp.apply(jnew);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return on;
		};
		return this;
	}
	

	public JsonNode build() {

		ObjectNode on = om.createObjectNode();

		this.grab.stream()
				.map(Tuple.vmap(s ->{
					String sva=Objects.toString(props.get(s));
					if(sva.equals("null") && orig!=null && s.startsWith("/")){
						
						JsonNode nv=orig.at(s);
						if(!nv.isMissingNode()){
							return nv.asText();
						}
					}
					return sva;
				
				}))				
				.filter(t -> t.v() != null)
				.filter(t -> !t.v().equalsIgnoreCase("Unknown"))
				.filter(t -> !t.v().equalsIgnoreCase("null"))
				.forEach(t -> {
					JsonNode val = jsonParsers.getOrDefault(t.k(), (s) -> new TextNode(s))
							.apply(t.v());
					on.replace(t.k(), val);
				});
		
		if(orig!=null){
			return runAfter.apply(orig, on);
		}
		return on;
	}
	
	
	
	

	public JsonNodeMaker remove(String oprop) {
		props.remove(oprop);
		return this;
	}
}
