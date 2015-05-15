# MI-File #

MI-File (Metric Inverted File) allows you to perform approximate similarity search on huge datasets. As an example give a look at the [Image Similarity Search Engine](http://mi-file.isti.cnr.it/CophirSearch/), which allows you searching in a dataset of more than 100 millions images, that was built using the MI-File library.

The technique is based on the use of a space transformation where data objects are represented by ordered sequences of reference objects. The sequence of reference objects that represent a data object is ordered according to the distance of the reference objects from the data object being represented. Distance between two data objects is measured by computing the spearmann footrule distance between the two sequence of reference objects that represent them. The closer the two data objects the most similar the two sequence of reference objects. The index is based on the use of inverted files.

(More details on the technique can be found in the paper "MI-File: using inverted files for scalable approximate similarity search", by Giuseppe Amato, Claudio Gennaro, and Pasquale Savino, published on Multimedia Tools and Applications: http://dx.doi.org/10.1007/s11042-012-1271-1 and "Approximate Similarity Search in Metric Spaces Using Inverted Files", by Giuseppe Amato and Pasquale Savino, presented at Infoscale 2008: http://mi-file.isti.cnr.it/CophirSearch/MI-file.pdf)

Intructions on ho to quickly use the library can be found at [GettingStarted](GettingStarted.md)

Contact me: [Giuseppe Amato](http://www.nmis.isti.cnr.it/amato/)