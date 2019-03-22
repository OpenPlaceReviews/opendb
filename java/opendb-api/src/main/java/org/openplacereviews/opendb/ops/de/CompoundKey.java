package org.openplacereviews.opendb.ops.de;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.openplacereviews.opendb.OUtils;

public class CompoundKey implements Collection<String> {
	final String first;
	final String second;
	final int hashcode;
	final List<String> others;

	public CompoundKey(String first, String second) {
		int result = 1;
		this.first = first;
		if (first != null) {
			result = 31 * result + (first == null ? 0 : first.hashCode());
		}
		this.second = second;
		if (second != null) {
			result = 31 * result + (second == null ? 0 : second.hashCode());
		}
		this.hashcode = result;
		this.others = null;
	}

	public CompoundKey(int subInd, List<String> l) {
		int result = 1;
		String first = null;
		String second = null;
		List<String> others = null;
		for (int i = subInd; i < l.size(); i++) {
			String element = l.get(i);
			if (element == null) {
				throw new IllegalArgumentException("Primary key coudln't be null: " + l);
			}
			if (i == subInd) {
				first = element;
			} else if (i == subInd + 1) {
				second = element;
			} else if (i == subInd + 2) {
				others = new ArrayList<String>();
				others.add(element);
			} else {
				others.add(element);
			}
			result = 31 * result + (element == null ? 0 : element.hashCode());
		}
		this.first = first;
		this.second = second;
		this.hashcode = result;
		this.others = others;
	}

	@Override
	public int hashCode() {
		return hashcode;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CompoundKey)) {
			return false;
		}
		CompoundKey l2 = (CompoundKey) obj;
		if (!OUtils.equals(first, l2.first)) {
			return false;
		}
		if (!OUtils.equals(second, l2.second)) {
			return false;
		}
		if (!OUtils.equals(others, l2.others)) {
			return false;
		}
		return true;
	}

	@Override
	public Iterator<String> iterator() {
		return new Iterator<String>() {
			int nextInd = 0;

			private String getByInd(int ind) {
				if (ind <= 0) {
					return first;
				} else if (ind == 1) {
					return second;
				} else if (others != null && others.size() > ind - 2) {
					others.get(ind - 2);
				}
				return null;
			}

			@Override
			public boolean hasNext() {
				return getByInd(nextInd) != null;
			}

			@Override
			public String next() {
				return getByInd(nextInd++);
			}

		};
	}

	@Override
	public int size() {
		if (first == null) {
			return 0;
		}
		if (second == null) {
			return 1;
		}
		if (others == null) {
			return 2;
		}
		return 2 + others.size();
	}

	@Override
	public boolean isEmpty() {
		return first == null;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(String e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

}