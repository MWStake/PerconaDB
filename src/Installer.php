<?php

/*
 * Copyright (C) 2019  NicheWork, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Mark A. Hershberger <mah@nichework.com>
 */
namespace MediaWiki\Extension\PerconaDB;

use MysqlInstaller;
use Status;
use Wikimedia\Rdbms\Database;
use Wikimedia\Rdbms\DBConnectionError;

class Installer extends MysqlInstaller {
	/** @var string[] */
	public $supportedEngines = [ 'InnoDB' ];

	/**
	 * @return string
	 */
	public function getName() {
		return 'percona';
	}

	/**
	 * @return Status
	 */
	public function openConnection() {
		$status = Status::newGood();
		try {
			$dbh = Database::factory(
				'percona', [
					'host' => $this->getVar( 'wgDBserver' ),
					'user' => $this->getVar( '_InstallUser' ),
					'password' => $this->getVar( '_InstallPassword' ),
					'dbname' => false,
					'flags' => 0,
					'tablePrefix' => $this->getVar( 'wgDBprefix' )
				]
			);
			$status->value = $dbh;
		} catch ( DBConnectionError $e ) {
			$status->fatal( 'config-connection-error', $e->getMessage() );
		}

		return $status;
	}

		/**
		 * @return string
		 */
	public function getSettingsForm() {
		if ( $this->canCreateAccounts() ) {
			$noCreateMsg = false;
		} else {
			$noCreateMsg = 'config-db-web-no-create-privs';
		}
		$settingsForm = $this->getWebUserBox( $noCreateMsg );

		// Do engine selector
		$engines = $this->getEngines();
		// If the current default engine is not supported, use an engine that is
		if ( !in_array( $this->getVar( '_MysqlEngine' ), $engines ) ) {
			$this->setVar( '_MysqlEngine', reset( $engines ) );
		}

		if ( count( $engines ) >= 2 ) {
			// getRadioSet() builds a set of labeled radio buttons.
			// For grep: The following messages are used as the item labels:
			// config-mysql-innodb, config-mysql-myisam
			$settingsForm .= $this->getRadioSet( [
													'var' => '_MysqlEngine',
													'label' => 'config-mysql-engine',
													'itemLabelPrefix' => 'config-mysql-',
													'values' => $engines,
													'itemAttribs' => [
														'InnoDB' => [
															'class' => 'hideShowRadio',
															'rel' => 'dbMyisamWarning'
														]
													]
			] );
			$settingsForm .= $this->parent->getHelpBox( 'config-mysql-engine-help' );
		}

		// If the current default charset is not supported, use a charset that is
		$charsets = $this->getCharsets();
		if ( !in_array( $this->getVar( '_MysqlCharset' ), $charsets ) ) {
			$this->setVar( '_MysqlCharset', reset( $charsets ) );
		}

		return $settingsForm;
	}
}
