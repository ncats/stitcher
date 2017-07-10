package controllers;

import java.io.ByteArrayOutputStream;
import java.text.NumberFormat;
import java.awt.Dimension;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

import chemaxon.struc.Molecule;
import org.freehep.graphicsio.svg.SVGGraphics2D;

import gov.nih.ncgc.chemical.Chemical;
import gov.nih.ncgc.chemical.ChemicalAtom;
import gov.nih.ncgc.chemical.ChemicalFactory;
import gov.nih.ncgc.chemical.ChemicalRenderer;
import gov.nih.ncgc.chemical.DisplayParams;
import gov.nih.ncgc.nchemical.NchemicalRenderer;
import gov.nih.ncgc.jchemical.Jchemical;

public class Util {
    public static String format (Number value) {
        if (value != null)
            return NumberFormat.getInstance().format(value);
        return "";
    }
    
    public static int[] paging (int rowsPerPage, int page, int total) {
        //last page
        int max = (total+ rowsPerPage-1)/rowsPerPage;
        if (page < 0 || page > max) {
            return new int[0];
        }
        
        int[] pages;
        if (max <= 10) {
            pages = new int[max];
            for (int i = 0; i < pages.length; ++i)
                pages[i] = i+1;
        }
        else if (page >= max-3) {
            pages = new int[10];
            pages[0] = 1;
            pages[1] = 2;
            pages[2] = 0;
            for (int i = pages.length; --i > 2; )
                pages[i] = max--;
        }
        else {
            pages = new int[10];
            int i = 0;
            //0-7 set to +1
            for (; i < 7; ++i)
                pages[i] = i+1;
            //if the page is larger than 7 (last 3 page)
            //
            if (page >= pages[i-1]) {
                // now shift
                pages[--i] = page;
                while (i-- > 0)
                    pages[i] = pages[i+1]-1;
                pages[0] = 1;
                pages[1] = 2;
                pages[2] = 0;
            }
            pages[8] = max-1;
            pages[9] = max;
        }
        
        return pages;
    }

    public static byte[] renderMol (Molecule mol, String format,
                                    int size, int[] amap)
        throws Exception {
        Chemical chem = new Jchemical (mol);
        DisplayParams dp = DisplayParams.DEFAULT();
        
        //chem.reduceMultiples();
        boolean highlight=false;
        if(amap!=null && amap.length>0){
            ChemicalAtom[] atoms = chem.getAtomArray();
            for (int i = 0; i < Math.min(atoms.length, amap.length); ++i) {
                atoms[i].setAtomMap(amap[i]);
                if(amap[i]!=0){
                    dp = dp.withSubstructureHighlight();
                    highlight=true;
                }
            }
        }
        
        if(/*size>250 &&*/ !highlight){
            if (chem.hasStereoIsomers())
                dp.changeProperty(DisplayParams.PROP_KEY_DRAW_STEREO_LABELS,
                                  true);
        }

        ChemicalRenderer render = new NchemicalRenderer ();
        render.setDisplayParams(dp);
        render.addDisplayProperty("TOP_TEXT");
        render.addDisplayProperty("BOTTOM_TEXT");

        ByteArrayOutputStream bos = new ByteArrayOutputStream ();       
        if (format.equals("svg")) {
            SVGGraphics2D svg = new SVGGraphics2D
                (bos, new Dimension (size, size));
            svg.startExport();
            render.renderChem(svg, chem, size, size, false);
            svg.endExport();
            svg.dispose();
        }
        else {
            BufferedImage bi = render.createImage(chem, size);
            ImageIO.write(bi, "png", bos); 
        }
        
        return bos.toByteArray();
    }
}
