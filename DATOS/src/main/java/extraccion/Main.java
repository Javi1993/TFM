package extraccion;

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
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

	private void showButtonDemo(){

		headerLabel.setText("Elija la fuente donde extraer:"); 

		JButton extraerButton = new JButton("Todas");        
		JButton datosButton = new JButton("DatosGobEs");
		JButton mambButton = new JButton("Mambiente");
		mambButton.setHorizontalTextPosition(SwingConstants.LEFT);   

		extraerButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				dgES.getDatosGobEs();
				new Mambiente();
				new Limpieza().separacionCarpetas();
				new Almacenar(dgES.getDataset_ID());
				statusLabel.setText("Proceso finalizado.");
				Funciones.vaciarDocuments();
			}          
		});

		datosButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				dgES.getDatosGobEs();
				new Limpieza().separacionCarpetas();
				new Almacenar(dgES.getDataset_ID());
				statusLabel.setText("Proceso finalizado.");
				Funciones.vaciarDocuments();
			}
		});

		mambButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				new Mambiente();
				new Limpieza().separacionCarpetas();
				new Almacenar(null);
				statusLabel.setText("Proceso finalizado.");
				Funciones.vaciarDocuments();
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

