package NIO;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;

public class SelectorFactory {
	protected static long timeout = 5000;
	protected static int maxSelectors = 20;
	private final static Stack<Selector> selectors = new Stack<Selector>();

	static {
		try {
			for (int i = 0; i < maxSelectors; i++)
				selectors.add(Selector.open());
		} catch (IOException ex) {
			; // do nothing.
		}
	}

	public final static Selector getSelector() {
		synchronized (selectors) {
			Selector s = null;
			try {
				if (selectors.size() != 0)
					s = selectors.pop();
			} catch (EmptyStackException ex) {
			}

			int attempts = 0;
			try {
				while (s == null && attempts < 2) {
					selectors.wait(timeout);
					try {
						if (selectors.size() != 0)
							s = selectors.pop();
					} catch (EmptyStackException ex) {
						break;
					}
					attempts++;
				}
			} catch (InterruptedException ex) {
			}
			;
			return s;
		}
	}

	public final static void returnSelector(Selector s) {
		synchronized (selectors) {
			selectors.push(s);
			if (selectors.size() == 1)
				selectors.notify();
		}
	}

}
