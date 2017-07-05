package de.alkern.graphulo.connected_components.analysis;public class SizeTyp{enum SizeType {
        EDGES("edges"), NODES("nodes");

        private java.lang.String name;

        SizeType(java.lang.String name) {
            this.name = name;
        }

        @java.lang.Override
        public java.lang.String toString() {
            return name;
        }
    }}