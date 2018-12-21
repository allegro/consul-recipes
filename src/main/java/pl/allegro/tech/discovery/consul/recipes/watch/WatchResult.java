package pl.allegro.tech.discovery.consul.recipes.watch;

import java.util.Objects;
import java.util.function.Function;

public class WatchResult<T> {

    private final long index;

    private final T body;

    public WatchResult(long index, T body) {
        this.index = index;
        this.body = body;
    }

    public long getIndex() {
        return index;
    }

    public T getBody() {
        return body;
    }

    public <R> WatchResult<R> map(Function<T, R> mapper) {
        return new WatchResult<>(index, mapper.apply(body));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WatchResult<?> that = (WatchResult<?>) o;
        return index == that.index && Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, body);
    }
}
