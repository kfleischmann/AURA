package de.tuberlin.aura.core.record.tuples;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

/**
 *
 */
public final class Tuple4<T0, T1, T2, T3> extends AbstractTuple {

    private static final long serialVersionUID = -1L;

    public T0 _0;

    public T1 _1;

    public T2 _2;

    public T3 _3;

    public Tuple4() {
        this(null, null, null, null);
    }

    public Tuple4(final T0 _0, final T1 _1, final T2 _2, final T3 _3) {

        this._0 = _0;

        this._1 = _1;

        this._2 = _2;

        this._3 = _3;
    }

    public Tuple4(final Tuple4<T0, T1, T2, T3> t) {
        this(t._0, t._1, t._2, t._3);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(final int pos) {
        switch(pos) {
            case 0: return (T) this._0;
            case 1: return (T) this._1;
            case 2: return (T) this._2;
            case 3: return (T) this._3;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(final T value, final int pos) {
        switch(pos) {
            case 0:
                this._0 = (T0) value;
                break;
            case 1:
                this._1 = (T1) value;
                break;
            case 2:
                this._2 = (T2) value;
                break;
            case 3:
                this._3 = (T3) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int length() {
        return 4;
    }

    @Override
    public Iterator<Object> iterator() {
        return Arrays.asList(new Object[]{_0, _1, _2, _3}).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(final AbstractTuple t) {
        if (this == t)
            return 0;
        final Tuple4<T0,T1,T2,T3> o = (Tuple4<T0,T1,T2,T3>)t;
        final ComparisonChain cc = ComparisonChain.start();
        if (_0 instanceof Comparable)
            cc.compare((Comparable<?>)_0, (Comparable<?>) o._0);
        if (_1 instanceof Comparable)
            cc.compare((Comparable<?>)_1, (Comparable<?>) o._1);
        if (_2 instanceof Comparable)
            cc.compare((Comparable<?>)_2, (Comparable<?>) o._2);
        if (_3 instanceof Comparable)
            cc.compare((Comparable<?>)_3, (Comparable<?>) o._3);
        return cc.result();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_0 == null) ? 0 : _0.hashCode());
        result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
        result = prime * result + ((_2 == null) ? 0 : _2.hashCode());
        result = prime * result + ((_3 == null) ? 0 : _3.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("unchecked")
        final Tuple4<T0,T1,T2,T3> other = (Tuple4<T0,T1,T2,T3>) obj;
        if (_0 == null) {
            if (other._0 != null)
                return false;
        } else if (!_0.equals(other._0))
            return false;
        if (_1 == null) {
            if (other._1 != null)
                return false;
        } else if (!_1.equals(other._1))
            return false;
        if (_2 == null) {
            if (other._2 != null)
                return false;
        } else if (!_2.equals(other._2))
            return false;
        if (_3 == null) {
            if (other._3 != null)
                return false;
        } else if (!_3.equals(other._3))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "(" + _0 + "," + _1 + "," + _2 + "," + _3 + ")";
    }
}
