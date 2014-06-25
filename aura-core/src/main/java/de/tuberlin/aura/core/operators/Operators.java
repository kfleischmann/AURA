package de.tuberlin.aura.core.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.task.spi.IRecordReader;

/**
 *
 */
public final class Operators {

    // Disallow instantiation.
    private Operators() {}

    /**
     *
     */
    public static enum OperatorType {

        OPERATOR_MAP
    }

    /**
     *
     */
    public static final class OperatorFactory {

        public static <I,O> IOperator createOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor) {
            // sanity check.
            if (operatorDescriptor == null)
                throw new IllegalArgumentException("operatorDescriptor == null");

            switch (operatorDescriptor.operatorType) {

                case OPERATOR_MAP: {

                    final UnaryUDFFunction<I,O> udfFunction;
                    try {
                        udfFunction = (UnaryUDFFunction<I,O>)operatorDescriptor.getUserCodeClasses().get(0).getConstructor().newInstance();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }

                    return new MapOperator<I,O>(operatorDescriptor, new ArrayList<IOperator>(), udfFunction);
                }

                default: {
                    throw new IllegalStateException("operator type not known");
                }
            }
        }
    }

    /**
     *
     */
    public static interface UnaryUDFFunction<I,O> {

        public abstract O apply(final I in);
    }

    /**
     *
     */
    public static interface IOperator extends Serializable {

        public abstract void open() throws Throwable;

        public abstract Object next() throws Throwable;

        public abstract void close() throws Throwable;
    }

    /**
     *
     */
    public static abstract class AbstractOperator implements IOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        protected final List<IOperator> childOperators;

        private final Descriptors.OperatorNodeDescriptor operatorDescriptor;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                                final List<IOperator> childOperators) {

            this.childOperators = childOperators;

            this.operatorDescriptor = operatorDescriptor;
        }

        public AbstractOperator(final List<IOperator> childOperators) {
            this(null, childOperators);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addChild(final IOperator operator) {
            // sanity check.
            if (operator == null)
                throw new IllegalArgumentException("operator == null");

            childOperators.add(operator);
        }

        public void open() throws Throwable {
        }

        public void close() throws Throwable {
        }

        public Descriptors.OperatorNodeDescriptor getOperatorDescriptor() {
            return operatorDescriptor;
        }
    }

    /**
     *
     */
    public static abstract class AbstractUDFOperator<I,O> extends AbstractOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        protected final UnaryUDFFunction<I,O> udfFunction;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public AbstractUDFOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                                   final List<IOperator> childOperators,
                                   final UnaryUDFFunction<I,O> udfFunction) {
            super(operatorDescriptor, childOperators);

            // sanity check.
            if (udfFunction == null)
                throw new IllegalArgumentException("udfFunction == null");

            this.udfFunction = udfFunction;
        }
    }

    /**
     *
     */
    public static final class GateReaderOperator extends AbstractOperator {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IRecordReader recordReader;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public GateReaderOperator(final IRecordReader recordReader) {
            super(null);
            // sanity check.
            if (recordReader == null)
                throw new IllegalArgumentException("recordReader == null");

            this.recordReader = recordReader;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public Object next() {
            return recordReader.readObject();
        }
    }

    /**
     *
     */
    public static final class MapOperator<I,O> extends AbstractUDFOperator<I,O> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public MapOperator(final Descriptors.OperatorNodeDescriptor operatorDescriptor,
                           final List<IOperator> childOperators,
                           final UnaryUDFFunction<I, O> udfFunction) {

            super(operatorDescriptor, childOperators, udfFunction);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable{

            // sanity check.
            if (childOperators.size() != 1)
                throw new IllegalStateException("childOperators.size() != 1");

            childOperators.get(0).open();
        }

        @Override
        public Object next() throws Throwable{
            final I input = (I) childOperators.get(0).next();
            if (input != null)
                return udfFunction.apply(input);
            else
                return null;
        }

        @Override
        public void close() throws Throwable{
            childOperators.get(0).close();
        }
    }
}
