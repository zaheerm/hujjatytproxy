<?php
/**
 * Plugin Name: Hujjat Livestream
 * Plugin URI:  https://github.com/zaheerm/hujjatytproxy/wp_hujjat_livestream
 * Description: Module to allow real-time status of live streams at KSIMC of London
 * Version:     1.0.3
 * Author:      Zaheer Abbas Merali
 * Author URI:  https://zaheer.merali.org
 * Text Domain: 
 * Domain Path: 
 * License:     Apache
 */

 // If this file is called directly, abort.
if ( ! defined( 'WPINC' ) ) {
	die;
}

function wp_hujjat_livesteam_enqueue_scripts( $hook ) {
    wp_enqueue_script('wp_hujjat_livestream.js', plugins_url( 'wp_hujjat_livestream.js', __FILE__), array('jquery'));
}
add_action( 'wp_enqueue_scripts', 'wp_hujjat_livesteam_enqueue_scripts' );
?>