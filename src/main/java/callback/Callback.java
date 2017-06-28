package callback;

/**
 * Created by weizhenyi on 2017/6/25.
 */
public interface Callback {
	public <T> Object execute(T ... args);
}
