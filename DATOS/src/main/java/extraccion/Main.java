package extraccion;

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import funciones.Funciones;
import preprocesamiento.Almacenar;
import preprocesamiento.Limpieza;

public class Main {

	private JFrame mainFrame;
	private JLabel headerLabel;
	private JLabel statusLabel;
	private JPanel controlPanel;
	private DatosGobES dgES;

	public Main(){
		prepareGUI();
		dgES = new DatosGobES();
	}

	public static void main(String[] args) {
		Funciones.loadPropierties();
		Main main = new Main();
		main.showButtonDemo();
		Funciones.vaciarDocuments();
	}

	private void prepareGUI(){
		mainFrame = new JFrame("TFM: Javier García");
		mainFrame.setSize(400,400);
		mainFrame.setLayout(new GridLayout(3, 1));
		mainFrame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent windowEvent){
				System.exit(0);
			}        
		});    
		headerLabel = new JLabel("", JLabel.CENTER);        
		statusLabel = new JLabel("",JLabel.CENTER);    

		statusLabel.setSize(350,100);

		controlPanel = new JPanel();
		controlPanel.setLayout(new FlowLayout());

		mainFrame.add(headerLabel);
		mainFrame.add(controlPanel);
		mainFrame.add(statusLabel);
		mainFrame.setVisible(true);  
	}

	private static ImageIcon createImageIcon(String path, 
			String description) {
		java.net.URL imgURL = Main.class.getResource(path);
		if (imgURL != null) {
			return new ImageIcon(imgURL, description);
		} else {            
			System.err.println("No se encuentra el archivo: " + path);
			return null;
		}
	}   

	private void showButtonDemo(){

		headerLabel.setText("Elija la acción a realizar:"); 

		//resources folder should be inside SWING folder.
		ImageIcon icon = createImageIcon("/resources/java_icon.png","Java");

		JButton extraerButton = new JButton("Extraer");        
		JButton datosButton = new JButton("DatosGobEs", icon);
		JButton mambButton = new JButton("Mambiente", icon);
		mambButton.setHorizontalTextPosition(SwingConstants.LEFT);   

		extraerButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				statusLabel.setText("Comprobando y descargando datasets...");
				dgES.getDatosGobEs();
				new Mambiente();
				new Limpieza().separacionCarpetas();
				new Almacenar(dgES.getDataset_ID());
			}          
		});

		datosButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				//aqui llamar a funcion del boton
				statusLabel.setText("Comprobando y descargando datasets de http://www.datos.gob.es...");
				dgES.getDatosGobEs();
				new Limpieza().separacionCarpetas();
				new Almacenar(dgES.getDataset_ID());
			}
		});

		mambButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				//aqui llamar a funcion del boton
				statusLabel.setText("Comprobando y descargando datasets de http://www.mambiente.munimadrid.es/...");
				new Mambiente();
				new Limpieza().separacionCarpetas();
				new Almacenar(null);
			}
		});

		controlPanel.add(extraerButton);
		controlPanel.add(datosButton);
		controlPanel.add(mambButton);       

		mainFrame.pack();
		mainFrame.setLocationRelativeTo(null); 
		mainFrame.setVisible(true);  
	}
}

