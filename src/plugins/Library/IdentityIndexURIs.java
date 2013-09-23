/* This code is part of Libray. It is 
 * used by Curator plugin */

package plugins.Library;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;

import freenet.keys.FreenetURI;
import freenet.keys.InsertableClientSSK;
import freenet.pluginmanager.PluginRespirator;
import freenet.support.Logger;
import freenet.support.io.Closer;

/**
 * Based on SpiderIndexURI
 * @author leuchtkaefer
 */

class IdentityIndexURIs {
	private FreenetURI privURI;
	private FreenetURI pubURI;
		
	private long edition;
	private final File workingDir;
	
	private final PluginRespirator pr;
	private FreenetURI suggestedInsertURI;
	
	static final String PRIV_URI_FILENAME = "library.index.privkey"; //Used by CuratorIndexURI
	static final String PUB_URI_FILENAME = "library.index.pubkey"; //Used by CuratorIndexURI
	static final String INDEX_FILENAME_EXTENSION = ".yml";
	
	IdentityIndexURIs(PluginRespirator pr, File workingDir, String iURI){
		this.pr = pr;
		this.workingDir = workingDir;
		try {
			this.suggestedInsertURI =  new FreenetURI(iURI).setDocName(workingDir.getName()+INDEX_FILENAME_EXTENSION);//this.suggestedInsertURI.setDocName(workingDir.getName()+INDEX_FILENAME_EXTENSION);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	synchronized long setEdition(long newEdition) {
		if(newEdition < edition) return edition;
		else return edition = newEdition;
	}
	
	synchronized FreenetURI loadSSKURIs() { 
		if(privURI == null) {	
			File f = new File(workingDir,PRIV_URI_FILENAME); //MULTIPLE IDENTITIES (+) leuchtkaefer
			FileInputStream fis = null;
			InsertableClientSSK privkey = null;
			boolean newPrivKey = false;
			try {
				File fEd = new File(workingDir,IdentityIndexUploader.EDITION_FILENAME);
				fis = new FileInputStream(fEd);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				try {
					edition = Long.parseLong(br.readLine());
				} catch (NumberFormatException e) {
					edition = Long.valueOf(0); 
				}
				System.out.println("Edition: "+edition);
				fis.close();
				fis = null;
			} catch (IOException e) {
				// Ignore
				edition = Long.valueOf(0); 
			} finally {
				Closer.close(fis);
			}
			try {
				fis = new FileInputStream(f);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				
				this.privURI = new FreenetURI(br.readLine());
				System.out.println("Read old privkey");
				this.pubURI = privURI.deriveRequestURIFromInsertURI(); 
				System.out.println("Recovered URI from disk");
				fis.close();
				fis = null;
			} catch (IOException e) {
				// Ignore
			} finally {
				Closer.close(fis);
			}

			if(privURI == null) { 		
				try {
					privURI = suggestedInsertURI.sskForUSK();
					pubURI = suggestedInsertURI.deriveRequestURIFromInsertURI().sskForUSK();
					newPrivKey = true;
				} catch (MalformedURLException e) {
					e.printStackTrace();
					Logger.error(this, "Failed to create insertable client ssk key");
				} 
			}
			FileOutputStream fos = null;
			if(newPrivKey) {
				try {
					File fPriv = new File(workingDir,PRIV_URI_FILENAME);
					fos = new FileOutputStream(fPriv); 
					OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
					osw.write(privURI.toASCIIString().split("-0")[0]);
					osw.close();
					fos = null;
				} catch (IOException e) {
					Logger.error(this, "Failed to write new private key");
				} finally {
					Closer.close(fos);
				}
				try {
					File fPub = new File(workingDir,PUB_URI_FILENAME);
					fos = new FileOutputStream(fPub);
					OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
					osw.write(pubURI.toASCIIString().split("-0")[0]);
					osw.close();
					fos = null;
				} catch (IOException e) {
					Logger.error(this, "Failed to write new pubkey", e);
				} finally {
					Closer.close(fos);
				}
			}
			

		}
		return privURI;
	}

	synchronized FreenetURI getPrivateUSK() {
		return loadSSKURIs().setKeyType("USK").setSuggestedEdition(edition);
	}

	/** Will return edition -1 if no successful uploads so far, otherwise the correct edition. */
	synchronized FreenetURI getPublicUSK() {
		loadSSKURIs();
		return pubURI.setKeyType("USK").setSuggestedEdition(getLastUploadedEdition());
	}

	private synchronized long getLastUploadedEdition() {
		/** If none uploaded, return -1, otherwise return the last uploaded version. */
		return edition-1;
	}

}
