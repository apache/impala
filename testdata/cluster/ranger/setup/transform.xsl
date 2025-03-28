<?xml version="1.0"?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
               xmlns:beans="http://www.springframework.org/schema/beans"
               xmlns:security="http://www.springframework.org/schema/security"
               version="1.0">

    <xsl:template match="/|node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <!-- Look for the following patterns in security-applicationContext.xml and
    comment out the matching nodes. -->
    <xsl:template match="
            beans:beans/security:http[@pattern='/service/assets/policyList/*'] |
            beans:beans/security:http[@pattern='/service/assets/resources/grant'] |
            beans:beans/security:http[@pattern='/service/assets/resources/revoke'] |
            beans:beans/security:http[@pattern='/service/gds/download/*'] |
            beans:beans/security:http[@pattern='/service/plugins/policies/download/*'] |
            beans:beans/security:http[@pattern='/service/plugins/services/grant/*'] |
            beans:beans/security:http[@pattern='/service/plugins/services/revoke/*'] |
            beans:beans/security:http[@pattern='/service/tags/download/*'] |
            beans:beans/security:http[@pattern='/service/roles/download/*'] |
            beans:beans/security:http[@pattern='/service/xusers/download/*']">
        <xsl:text disable-output-escaping="yes">&lt;!-- </xsl:text>
        <xsl:copy-of select="."/>
        <xsl:text disable-output-escaping="yes"> --&gt;</xsl:text>
    </xsl:template>
</xsl:transform>
